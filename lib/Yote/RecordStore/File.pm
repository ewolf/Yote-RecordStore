package Yote::RecordStore::File;

# TODO - verify perldoc
#      - implement/enable transactions
#      - shed/keep YAML?
#      - subclass Yote::RecordStore?
#      - read/write locking
#      - testing
#      - consider changing fetch return value to omit updated and created 
#        times


=head1 NAME

 Yote::RecordStore::File - Simple store for text and byte data

=head1 SYNPOSIS

 use Yote::RecordStore::File;

 $store = Yote::RecordStore::File->init_store( DIRECTORY => $directory, MAX_FILE_SIZE => 20_000_000_000 );
 $data = "TEXT OR BYTES";

 # the first record id is 1
 my $id = $store->stow( $data );

 my $val = $store->fetch( $some_id );

 my $count = $store->entry_count;

 $store->lock( qw( FEE FIE FOE FUM ) ); # lock blocks, may not be called until unlock.

 $store->unlock; # unlocks all

 $store->delete_record( $id_to_remove ); #deletes the old record

 $reopened_store = Yote::RecordStore::File->open_store( $directory );

=head1 DESCRIPTION

Yote::RecordStore::File is a simple way to store serialized text or byte data.
It is written entirely in perl with no non-core dependencies.
It is designed to be both easy to set up and easy to use.

It adheres to a RecordStore interface so other implementations exist using
other technologies rather than simple binary file storage.

Transactions (see below) can be created that stow records.
They come with the standard commit and rollback methods. If a process dies
in the middle of a commit or rollback, the operation can be reattempted.
Incomplete transactions are obtained by the store's 'list_transactions'
method.

Yote::RecordStore::File operates directly and instantly on the file system.
It is not a daemon or server and is not thread safe. It can be used
in a thread safe manner if the controlling program uses locking mechanisms,
including the locks that the store provides.

=head1 METHODS

=cut

use strict;
use warnings;
no warnings 'numeric';
no warnings 'uninitialized';

use Data::Dumper;
use Fcntl qw( :flock SEEK_SET );
use File::Path qw(make_path);
use Scalar::Util qw(openhandle);
use Time::HiRes qw(time);

use Yote::RecordStore::File::Silo;
use Yote::RecordStore::File::Transaction;

use vars qw($VERSION);

$VERSION = '6.06';
my $SILO_VERSION = '6.00';

use constant {

    # the record store thingy is an 
    # array, and these are the indexes of
    # its components
    DIRECTORY              => 0,
    ACTIVE_TRANS_FILE      => 1,
    MAX_FILE_SIZE          => 2,
    MIN_SILO_ID            => 3,
    INDEX_SILO             => 4,
    SILOS                  => 5,
    TRANSACTION_INDEX_SILO => 6,
    TRANSACTION            => 7,
    HEADER_SIZE            => 8,
    LOCK_FH                => 9,
    LOCKS                  => 10,
    MAX_SILO_ID            => 11,

    # record state
    RS_ACTIVE         => 1,
    RS_DEAD           => 2,
    RS_IN_TRANSACTION => 3,

    # transactions
    TR_ACTIVE         => 1,
    TR_IN_COMMIT      => 2,
    TR_IN_ROLLBACK    => 3,
    TR_COMPLETE       => 4,
};

=head2 open_store( options )

Constructs a data store according to the options.

=cut

sub open_store {
    my( $cls, $dir ) = @_;

    unless( -d $dir ) {
        _make_path( $dir, 'base' ) or return undef;
    }

    my $max_file_size = $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE;
    my $min_file_size = $Yote::RecordStore::File::Silo::DEFAULT_MIN_FILE_SIZE;

    my $max_silo_id = int( log( $max_file_size ) / log( 2 ));
    $max_silo_id++ if 2 ** $max_silo_id < $max_file_size; #provide a floor for rounding errors

    my $min_silo_id = int( log( $min_file_size ) / log( 2 ));
    $min_silo_id++ if 2 ** $min_silo_id < $min_file_size; #provide a floor for rounding errors

    my $lockfile = "$dir/LOCK";
    my $lock_fh;

    my $silo_dir  = "$dir/data_silos";
    my $lock_dir  = "$dir/user_locks";
    my $trans_dir = "$dir/transactions";
    
    $lock_fh = _open (-e $lockfile ? '+<' : '>', $lockfile);
    unless ($lock_fh) {
        $@ = "Error opening lockfile '$lockfile' : $@ $!";
        return undef;
    }

    _flock( $lock_fh, LOCK_EX );
    $lock_fh->autoflush(1);
    print $lock_fh "LOCK\n";
        
    my $vers_file = "$dir/VERSION";
    if( -e $vers_file ) {
        open my $vers_fh, '<', $vers_file;
        my $existing_vers = <$vers_fh>;

        if ($existing_vers < 6.06) {
            $@ = "Cannot open recordstore at $dir with version $existing_vers";
            close $vers_fh;
            return undef;
        }
        close $vers_fh;
    }
    else {
        open my $vers_fh, '>', $vers_file;
#        print STDERR "creating ".__PACKAGE__." version $VERSION\n";
        print $vers_fh "$VERSION\n";
        close $vers_fh;
    }
    _make_path( $silo_dir, 'silo' ) or return undef;
    _make_path( $lock_dir, 'lock' ) or return undef;
    _make_path( $trans_dir, 'transaction' ) or return undef;

    my $index_silo = $cls->open_silo( "$dir/index_silo",
                                      "ILQQ"); #silo id, id in silo, last updated time, created time
        

    $index_silo || return undef;

    my $transaction_index_silo = $cls->open_silo( "$dir/transaction_index_silo",
                                                  "IQ" ); #state, time

    $transaction_index_silo || return undef;

    my $silos = [];

    for my $silo_id ($min_silo_id..$max_silo_id) {
        my $silo = $silos->[$silo_id] = $cls->open_silo( "$silo_dir/$silo_id",
                                                         'ILLa*',  # status, id, data-length, data
                                                         2 ** $silo_id ); #size
        $silo || return undef;
    }

    my $header = pack( 'ILL', 1,2,3 );
    my $header_size = do { use bytes; length( $header ) };

    my $store = bless [
        $dir,
        "$dir/ACTIVE_TRANS",
        $max_file_size,
        $min_silo_id,
        $index_silo,
        $silos,
        $transaction_index_silo,
        undef,
        $header_size, # the ILL from ILLa*
        $lock_fh,
        {},
        $max_silo_id,
    ], $cls;

    $store->fix_transactions;

    _flock( $lock_fh, LOCK_UN );

    return $store;
} #open_store

sub _open {
    my( $mod, $file) = @_;
    open my ($fh), $mod, $file;
    return $fh;
}

sub _flock {
    my ($fh, $flags) = @_;
    flock( $fh, $flags );
}

sub _openhandle {
    my $fh = shift;
    openhandle( $fh );
}

sub directory {
    shift->[DIRECTORY];
}

sub lock {
    my $self = shift;
    unless (_openhandle( $self->[LOCK_FH] )) {
        my $lockfile = "$self->[DIRECTORY]/LOCK";
        $self->[LOCK_FH] = _open ( -e $lockfile ? '+<' : '>', $lockfile );
        $self->[LOCK_FH] || return undef;
    }
    _flock( $self->[LOCK_FH], LOCK_EX ) || return undef;

    $self->fix_transactions;
    $self->reset;

    return 1;
}

sub unlock {
    my $self = shift;
    _flock( $self->[LOCK_FH], LOCK_UN ) || return undef;
    return 1;
}

sub fetch {
    my( $self, $id, $no_trans ) = @_;
    my $trans = $self->[TRANSACTION];
    if( $trans && ! $no_trans ) {
        if( $trans->{state} != TR_ACTIVE ) {
            $@ = "Transaction is in a bad state. Cannot fetch";
            return undef;
        }
        return $trans->fetch( $id );
    }

    if( $id > $self->record_count ) {
        return undef;
    }

    my( $silo_id, $id_in_silo, $update_time, $creation_time ) = @{$self->[INDEX_SILO]->get_record($id)};
    if( $silo_id ) {
        my $ret = $self->[SILOS]->[$silo_id]->get_record( $id_in_silo );
#        print STDERR "FETCH $id ($silo_id/$id_in_silo) : ".substr( $ret->[3], 0, $ret->[2] > 100 ? 100 : $ret->[2] )."\n";
        return $update_time, $creation_time, substr( $ret->[3], 0, $ret->[2] );
    }
#    print STDERR "FETCH $id (NULL)\n";
    return undef;
} #fetch

sub fetch_meta {
    my( $self, $id ) = @_;

    if( $id > $self->record_count ) {
        
        return undef;
    }

    my( $silo_id, $id_in_silo, $update_time, $creation_time ) = @{$self->[INDEX_SILO]->get_record($id)};
    return $update_time, $creation_time;
} #fetch_meta

sub stow {
    my $self = $_[0];
    my $id   = $_[2];

    my $trans = $self->[TRANSACTION];
    if( $trans ) {
        return $trans->stow( $_[1], $id );
    }

    my $index = $self->[INDEX_SILO];

    $index->ensure_entry_count( $id );
    if( defined $id && ($id < 1|| int($id) != $id)) {
        $@ = "The id for ".__PACKAGE__."->stow must be a supplied as a positive integer";
        return undef;
    }
    my( $old_silo_id, $old_id_in_silo, $old_creation_time );
    if( $id > 0 ) {
       ( $old_silo_id, $old_id_in_silo, undef, $old_creation_time ) = @{$index->get_record($id)};
    }
    else {
        $id = $index->next_id;
    }

    my $data_write_size = do { use bytes; length $_[1] };
    my $new_silo_id = $self->silo_id_for_size( $data_write_size );
    my $new_silo = $self->[SILOS][$new_silo_id];

    my $new_id_in_silo = $new_silo->push( [RS_ACTIVE, $id, $data_write_size, $_[1]] );

#    print STDERR "WRITE $id ($new_silo_id/$new_id_in_silo) --> ".substr($_[1],0,100)."\n";
    my $t = int(time * 1000);

    unless ($index->put_record( $id, [$new_silo_id,$new_id_in_silo, $t, $old_creation_time ? $old_creation_time : $t] )) {
        return undef;
    }

    if( $old_silo_id ) {
        $self->_vacate( $old_silo_id, $old_id_in_silo );
    }

    return $id;
} #stow

sub next_id {
    return shift->[INDEX_SILO]->next_id;
} #next_id

sub first_id {
    return 1;
}

sub delete_record {
    my( $self, $del_id ) = @_;
    my $trans = $self->[TRANSACTION];
    if( $trans ) {
        return $trans->delete_record( $del_id );
    }

    if( $del_id > $self->[INDEX_SILO]->entry_count ) {
        warn "Tried to delete past end of records";
        return undef;
    }
    my( $old_silo_id, $old_id_in_silo ) = @{$self->[INDEX_SILO]->get_record($del_id)};
    my $t = int(time * 1000);
    unless ($self->[INDEX_SILO]->put_record( $del_id, [0,0,$t,$t] )) {
        return undef;
    }

    if( $old_silo_id ) {
        $self->_vacate( $old_silo_id, $old_id_in_silo );
    }
} #delete_record


sub use_transaction {
return;
    my $self = shift;
    if( $self->[TRANSACTION] ) {
        warn __PACKAGE__."->use_transaction : already in transaction";
        return $self->[TRANSACTION];
    }
    my $tid = $self->[TRANSACTION_INDEX_SILO]->push( [TR_ACTIVE, int(time * 1000)] );
    $self->unlock;
    my $tdir = "$self->[DIRECTORY]/transactions/$tid";
    make_path( $tdir, { error => \my $err } );
    if( @$err ) { 
        $@ = join( ", ", map { values %$_ } @$err );
        return undef;
    }

    $self->[TRANSACTION] = Yote::RecordStore::File::Transaction->create( $self, $tdir, $tid );
    return $self->[TRANSACTION];
} #use_transaction

sub commit_transaction {
return;
    my $self = shift;
    my $trans = $self->[TRANSACTION];
    unless( $trans ) {
        $@ = __PACKAGE__."->commit_transaction : no transaction to commit";
        return undef;
    }
    my $trans_file = $self->[ACTIVE_TRANS_FILE];
    open my $trans_fh, '>', $trans_file;
    print $trans_fh " ";
    close $trans_fh;

    if ($trans->commit) {
        delete $self->[TRANSACTION];
        unlink $trans_file;
        $self->unlock;
        return 1;
    }
} #commit_transaction

sub rollback_transaction {
return;
    my $self = shift;
    my $trans = $self->[TRANSACTION];
    unless( $trans ) {
        $@ = __PACKAGE__."->rollback_transaction : no transaction to roll back";
        return undef;
    }
    my $trans_file = $self->[ACTIVE_TRANS_FILE];
    open my $trans_fh, '>', $trans_file;
    print $trans_fh " ";
    close $trans_fh;
    $trans->rollback;
    delete $self->[TRANSACTION];
    unlink $trans_file;
    $self->unlock;
    return 1;
} #rollback_transaction

sub index_silo {
    return shift->[INDEX_SILO];
}

sub silos {
    return [@{shift->[SILOS]}];
}

sub transaction_silo {
    return shift->[TRANSACTION_INDEX_SILO];
}

sub silos_entry_count {
    my $self = shift;
    my $silos = $self->silos;
    my $count = 0;
    for my $silo (grep {defined} @$silos) {
        $count += $silo->entry_count;
    }
    return $count;
}

sub record_count {
    return shift->[INDEX_SILO]->entry_count;
}

sub active_entry_count {
    my $self = shift;
    my $index = $self->index_silo;
    my $count = 0;
    for(1..$self->record_count) {
        my( $silo_id ) = @{$index->get_record( $_ )};
        ++$count if $silo_id;
    }
    return $count;
}

sub detect_version {
    my( $cls, $dir ) = @_;
    my $ver_file = "$dir/VERSION";
    my $source_version;
    if ( -e $ver_file ) {
        open( my $FH, "<", $ver_file );
        $source_version = <$FH>;
        chomp $source_version;
        close $FH;
    }
    return $source_version;
} #detect_version



sub _vacate {
    my( $self, $silo_id, $id_to_empty ) = @_;
    my $silo = $self->[SILOS][$silo_id];
    my $rc = $silo->entry_count;
    if( $id_to_empty == $rc ) {
        $silo->pop;
    } else {
        while( $rc > $id_to_empty ) {
            my( $state, $id ) = (@{$silo->get_record( $rc, 'IL' )});
            if( $state == RS_ACTIVE ) {
                $silo->copy_record($rc,$id_to_empty);
                # the following does not update the time field, it preserves it
                $self->[INDEX_SILO]->put_record( $id, [$silo_id,$id_to_empty], "IL" );
                $silo->pop;
                return;
            }
            elsif( $state == RS_DEAD ) {
                $silo->pop;
            }
            else {
                return;
            }
            $rc--;
        }
    }
    return 1;
} #_vacate

sub silo_id_for_size {
    my( $self, $data_write_size ) = @_;

    my $write_size = $self->[HEADER_SIZE] + $data_write_size;

    my $silo_id = int( log( $write_size ) / log( 2 ) );
    $silo_id++ if 2 ** $silo_id < $write_size;
    $silo_id = $self->[MIN_SILO_ID] if $silo_id < $self->[MIN_SILO_ID];
    return $silo_id;
} #silo_id_for_size

# ---------------------- private stuffs -------------------------

sub open_silo {
    my ($self, $silo_file, $template, $size, $max_file_size ) = @_;

    

    return Yote::RecordStore::File::Silo->open_silo( $silo_file,
                                                     $template,
                                                     $size,
                                                     $max_file_size );
}



sub _make_path {
    my( $dir, $msg ) = @_;
    make_path( $dir, { error => \my $err } );
    if( @$err ) {
        $@ = "unable to make $msg directory.". join( ", ", map { $_->{$dir} } @$err );
        return undef;
    }
    return 1;
}

sub reset {
    my $self = shift;
    my $index_silo = $self->index_silo;
    my $transaction_silo = $self->transaction_silo;
    my $silos = $self->silos;
    for my $silo ($index_silo, $transaction_silo, @$silos) {
        $silo && $silo->reset;
    }
}


sub fix_transactions {
return;
    my $self = shift;
    # check the transactions
    # if the transaction is in an incomplete state, fix it. Since the store is write locked
    # during transactions, the lock has expired if this point has been reached.
    # that means the process that made the lock has fallen.
    #
    # of course, do a little experiment to test this with two processes and flock when
    # one exits before unflocking.
    #
    my $transaction_index_silo = $self->transaction_silo;
    my $last_trans = $transaction_index_silo->entry_count;
    while( $last_trans ) {
        my( $state ) = @{$transaction_index_silo->get_record( $last_trans )};
        my $tdir = "$self->[DIRECTORY]/transactions/$last_trans";
        if( $state == TR_IN_ROLLBACK ||
                $state == TR_IN_COMMIT ) {
            # do a full rollback
            # load the transaction
            my $trans = Yote::RecordStore::File::Transaction->create( $self, $tdir, $last_trans );
            $trans->rollback;
            $transaction_index_silo->pop;
        }
        elsif( $state == TR_COMPLETE ) {
            $transaction_index_silo->pop;
        }
        else {
            return;
        }
        $last_trans--;
    }

} #fix_transactions


"I became Iggy because I had a sadistic boss at a record store. I'd been in a band called the Iguanas. And when this boss wanted to embarrass and demean me, he'd say, 'Iggy, get me a coffee, light.' - Iggy Pop";

__END__


Options

=over 2

=item directory

=item min_file_size - default is 4096

=item max_file_size - default is 2 gigs

=head2 reopen_store( directory )

Opens the existing store in the given directory.

=head2 fetch( id )

Returns the record associated with the ID. If the ID has no
record associated with it, undef is returned.

=head2 stow( data, optionalID )

This saves the text or byte data to the record store.
If an id is passed in, this saves the data to the record
for that id, overwriting what was there.
If an id is not passed in, it creates a new record store.

Returns the id of the record written to.

=head2 next_id

This sets up a new empty record and returns the
id for it.

=head2 delete_record( id )

Removes the entry with the given id from the store, freeing up its space.
It does not reuse the id.

=head2 lock( @names )

Adds an advisory (flock) lock for each of the unique names given.
This may not be called twice in a row without an unlock in between
and will return undef if that happens and set $@.

=head2 unlock

Unlocks all names locked by this thread

=head2 use_transaction()

Returns the current transaction. If there is no
current transaction, it creates one and returns it.

=head2 commit_transaction()

Commits the current transaction, if any.

=head2 rollback_transaction()

Rolls back the current transaction, if any.

=head2 entry_count

Returns how many record ids exist.

=head2 index_silo

Returns the index silo for this store. 
This method is not part of the record store interface.

=head2 max_file_size

Returns the max file size of any silo in bytes.
This method is not part of the record store interface.

=head2 silos

Returns a list of data silo objects where the data silo record
size is 2**index position. This means that the beginning of the list
will have undefs as there is a minimum silo size.
This method is not part of the record store interface.

=head2 transaction_silo

Returns the transaction silo for this store.
This method is not part of the record store interface.

=head2 active_entry_count

Returns how many record ids exist that have silo entries.
This method is not part of the record store interface.

=head2 silos_entry_count

Returns the number of entries in the data silos.

=head2 detect_version( $dir )

Tries to detect the version of the Yote::RecordStore::File in
the given directory, if any.

=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2015-2021 Eric Wolf. All rights reserved.
       This program is free software; you can redistribute it
       and/or modify it under the same terms as Perl itself.

=head1 VERSION
       Version 1.00  (July, 2021))

=cut


# ------------- for DEBUG ---------

sub _show_silo {
    my( $self, $txt, $temp ) = @_;

    my( @pairs ) = (['index',$self->[INDEX_SILO],"IL"], map { ["record $_", $self->[SILOS][$_],'IL'] } ($self->[MIN_SILO_ID]..$self->[MAX_SILO_ID]) );
    my $trans = $self->[TRANSACTION];
    if( $trans ) {
        push @pairs, ['trans stack',$trans->{stack_silo}];
    }
    print STDERR "\n";
    for my $pair (@pairs) {
        my( $title, $silo, $templ ) = @$pair;
        if( my $ec = $silo->entry_count ) {
            print STDERR " $title : $txt ". join(",", map { " ($_)[".join(",",@{$silo->get_record($_,$templ)} ).']' } (1..$ec) )."\n";
        }
    }
}
