package Yote::RecordStore;

# TODO - verify perldoc
#      - implement/enable transactions
#      - shed/keep YAML?
#      - subclass Yote::RecordStore?
#      - read/write locking
#      - testing
#      - consider changing fetch return value to omit updated and created
#        times


=head1 NAME

 Yote::RecordStore - Simple store for text and byte data

=head1 SYNPOSIS

 use Yote::RecordStore;

 $store = Yote::RecordStore->new( $directory );

 $store->open;

 $data = "TEXT OR BYTES";

 # the first record id is 1
 my $id = $store->stow( $data );

 my $val = $store->fetch( $some_id );

 my $count = $store->entry_count;

 $store->lock; #locks this recordstore

 $store->unlock; # unlocks all

 $store->delete_record( $id_to_remove ); #deletes the old record

 $reopened_store = Yote::RecordStore->open_store( $directory );

=head1 DESCRIPTION

Yote::RecordStore is a simple way to store serialized text or byte data.
It is written entirely in perl with no non-core dependencies.
It is designed to be both easy to set up and easy to use.

It adheres to a RecordStore interface so other implementations exist using
other technologies rather than simple binary file storage.

Transactions (see below) can be created that stow records.
They come with the standard commit and rollback methods. If a process dies
in the middle of a commit or rollback, the operation can be reattempted.
Incomplete transactions are obtained by the store's 'list_transactions'
method.

Yote::RecordStore operates directly and instantly on the file system.
It is not a daemon or server and is not thread safe. It can be used
in a thread safe manner if the controlling program uses locking mechanisms,
including the locks that the store provides.

=head1 METHODS

=cut

use v5.16.0;

no warnings 'numeric';
no warnings 'uninitialized';

use Carp 'longmess';
use Data::Dumper;
use File::Path qw(make_path);
use Time::HiRes qw(time);

use Yote::Locker;
use Yote::RecordStore::Silo;
use Yote::RecordStore::Transaction;

use vars qw($VERSION);

$VERSION = '6.06';
my $SILO_VERSION = '6.00';

use constant {

    # the record store thingy is an
    # array, and these are the indexes of
    # its components
    DIRECTORY              => 0,

    MAX_FILE_SIZE          => 1,
    MIN_SILO_ID            => 2,
    INDEX_SILO             => 3,
    SILOS                  => 4,
    TRANSACTION_INDEX_SILO => 5,
    TRANSACTION            => 6,
    HEADER_SIZE            => 7,
    MAX_SILO_ID            => 8,
    LOCKER                 => 9,
    ARGS                   => 10,

    # record state
    RS_ACTIVE         => 1,
    RS_DEAD           => 2,
    RS_IN_TRANSACTION => 3,

    # transactions
    TR_ACTIVE         => 4,
    TR_IN_COMMIT      => 5,
    TR_IN_ROLLBACK    => 6,
    TR_COMPLETE       => 7,
};

my $file_quanta = $Yote::RecordStore::Silo::DEFAULT_MIN_FILE_SIZE;
my $quanta_boost = 4;

=head2 open_store( directory )

Constructs a data store according to the options.

=cut

sub open_store {
    my( $pkg, @args ) = @_;

    my %args = @args == 1 ? ( directory => $args[0] ) : @args;

    my $dir  = $args{directory};
    my $locker = $args{locker};

    unless( -d $dir ) {
        _make_path( $dir, \my $err, 'base' );
        if (@$err) {
            _err( 'open_store', "unable to make base directory.". join( ", ", map { $_->{$dir} } @$err ) );
        }
    }

    my $max_file_size = $Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE;
    my $min_file_size = $Yote::RecordStore::Silo::DEFAULT_MIN_FILE_SIZE;

    my $max_silo_id = _silo_id_for_size( $max_file_size, 0, 1 );
    my $min_silo_id = _silo_id_for_size( $min_file_size, 0, 1 );

#print STDERR "SILOS FROM $min_silo_id ... $max_silo_id\n";
    my $silo_dir  = "$dir/data_silos";

    unless ($locker) {
        my $lockfile = "$dir/LOCK";
        $locker = Yote::Locker->new( $lockfile );
    }

    my $vers_file = "$dir/VERSION";
    if( -e $vers_file ) {
        my $vers_fh = _open ($vers_file, '<' );
        my $existing_vers = <$vers_fh>;

        if ($existing_vers < 6.06) {
            $@ = "Cannot open recordstore at $dir with version $existing_vers";
            close $vers_fh;
            die $@;
        }
        close $vers_fh;
    }
    else {
        my $vers_fh = _open ( $vers_file, '>');
        print $vers_fh "$VERSION\n";
        close $vers_fh;
    }

    _make_path( $silo_dir, \my $err, 'silo' );
    if (@$err) {
        _err( 'open_store', "unable to make silo directory.". join( ", ", map { $_->{$silo_dir} } @$err ) );
    }

    my $index_silo = $pkg->_open_silo(
        "$dir/index_silo",
        "ILQQ", #silo id, id in silo, last updated time, created time
        blocking => 1,
        );
    
    my $transaction_index_silo = $pkg->_open_silo(
        "$dir/transaction_index_silo",
        "IL", #state, trans id
        blocking => 1,
        ); 

    my $silos = [];

    my $header = pack( 'ILL', 1,2,3 );
    my $header_size = do { use bytes; length( $header ) };

    my $store = bless [
        $dir,                                   
        $max_file_size,
        $min_silo_id,
        $index_silo,
        $silos,
        $transaction_index_silo,
        undef,  # 7
        $header_size, # the ILL from ILLa*
        $max_silo_id,
        $locker,
        {%args},
    ], $pkg;

    $store->load_active_silos;

    $store->_fix_transactions;
    $store->_vacuum;

    $locker->unlock;

    return $store;
} #open_store

=item directory

return base directory for this recordstore.

=cut
sub directory {
    shift->[DIRECTORY];
}

=item is_locked

returns true if this recordstore is currntly locked.

=cut
sub is_locked {
    shift->[LOCKER]->is_locked;
}

=item lock

lock this recordstore.

=cut
sub lock {
    my $self = shift;
    my $ret = $self->[LOCKER]->lock;
    $ret && $self->_reset;
    return $ret;
}

=item unlock

unlock this recordstore.

=cut
sub unlock {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _warn( 'unlock', "store not locked" );
        return 1;
    }

    if ($self->[TRANSACTION]) {
        _err( 'unlock', "may not unlock with a pending transaction, either commit it or roll it back" );
    }

    $self->[LOCKER]->unlock;
}

=item fetch(id)

Returns the record by id.

=cut
sub fetch {
    my( $self, $id ) = @_;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'fetch', "record store not locked");
    }

    my $trans = $self->[TRANSACTION];
    if( $trans ) {
        return $trans->fetch( $id );
    }

    $self->_fetch( $id );
} #fetch

=item fetch_meta(id)

Returns update_time,creation_time of the record.

=cut

sub fetch_meta {
    my( $self, $id ) = @_;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'fetch_meta', "record store not locked");
    }

    if( $id > $self->record_count ) {
        return undef;
    }

    my( $silo_id, $idx_in_silo, $update_time, $creation_time ) = @{$self->[INDEX_SILO]->get_record($id)};
    return $update_time, $creation_time;
} #fetch_meta

=item stow( data, id )

stores the given data and returns the id. If not given
an id, it assigns the next free id to the item.

=cut
sub stow {
    my ($self, $data, $id ) = @_;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'stow', "record store not locked");
    }

    my $trans = $self->[TRANSACTION];
    if ($trans) {
        return $trans->stow( $data, $id );
    }

    return $self->_stow( $data, $id );
}


=item next_id

creates and returns a new id.

=cut
sub next_id {
    my $self = shift;
    unless ($self->[LOCKER]->is_locked) {
        _err( 'next_id', "record store not locked");
    }

    return $self->[INDEX_SILO]->next_id;
} #next_id

=item first_id

returns the first id in this store.

=cut
sub first_id {
    return 1;
}

=item delete_record(id)

Marks the record by id as dead.

=cut
sub delete_record {
    my( $self, $del_id ) = @_;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'delete_record', "record store not locked");
    }

    my $trans = $self->[TRANSACTION];

    if( $trans ) {
        return $trans->delete_record( $del_id );
    }
    return $self->_delete_record( $del_id );
} #delete_record

sub _delete_record {
    my( $self, $del_id ) = @_;

    if( $del_id > $self->[INDEX_SILO]->entry_count ) {
        _err( 'delete_record', "Tried to delete past end of records" );
    }
    my( $old_silo_id, $old_idx_in_silo ) = @{$self->[INDEX_SILO]->get_record($del_id)};
    my $t = _time();

    $self->_log( "$$ DELETE \%$del_id from $old_idx_in_silo/$old_silo_id $t" );

    $self->[INDEX_SILO]->put_record( $del_id, [0,0,$t,$t] );

    if( $old_silo_id ) {
        $self->_mark( $old_silo_id, $old_idx_in_silo, RS_DEAD );
    }
} #_delete_record

=item use_transaction



=cut
sub use_transaction {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'use_transaction', "record store not locked");
    }

    if( $self->[TRANSACTION] ) {
        _warn ('use_transaction', 'already in transaction');
        return $self->[TRANSACTION];
    }
    $self->[TRANSACTION] = Yote::RecordStore::Transaction->create( $self );

    return $self->[TRANSACTION];
} #use_transaction

=item commit_transaction

=cut

sub commit_transaction {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'commit_transaction', "record store not locked");
    }

    my $trans = $self->[TRANSACTION];

    unless( $trans ) {
        _err( 'commit_transaction',  'no transaction to commit' );
    }

    $trans->commit;
    $self->[TRANSACTION] = undef;
    return 1;

} #commit_transaction

=item rollback_transaction

=cut
sub rollback_transaction {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'rollback_transaction', "record store not locked");
    }

    my $trans = $self->[TRANSACTION];
    unless( $trans ) {
        _err( 'rollback_transaction',  'no transaction to roll back' );
    }
    $trans->rollback;
    $self->[TRANSACTION] = undef;
    return 1;
} #rollback_transaction

=item index_silo

returns the idexing silo object.

=cut
sub index_silo {
    return shift->[INDEX_SILO];
}

=item silos

returns array ref of all created storage silos.

=cut
sub silos {
    return [@{shift->[SILOS]}];
}

=item transaction_silo

returns the transaction silo object.

=cut
sub transaction_silo {
    return shift->[TRANSACTION_INDEX_SILO];
}

=item silos_entry_count

Returns the sum of entry counts (live or dead) for all silos.

=cut
sub silos_entry_count {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'silos_entry_count', "record store not locked");
    }

    my $silos = $self->silos;
    my $count = 0;
    for my $silo (grep {defined} @$silos) {
        $count += $silo->entry_count;
    }
    return $count;
}

=item record_count

Returns the number of all records, active or dead.

=cut

sub record_count {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'record_count', "record store not locked");
    }

    return $self->[INDEX_SILO]->entry_count;
}

=item active_entry_count

Returns the number of entries not marked as dead in the store.
This scans the index and returns a count of all entries with assigned
placements.

=cut
sub active_entry_count {
    my $self = shift;

    unless ($self->[LOCKER]->is_locked) {
        _err( 'active_entry_count', "record store not locked");
    }

    my $index = $self->index_silo;
    my $count = 0;
    for(1..$self->record_count) {
#        my( $silo_id ) = @{$index->get_record( $_ )};
        my( $silo_id, $iis ) = @{$index->get_record( $_ )};
        ++$count if $silo_id;
    }
    return $count;
}

=item use_transaction( directory )

package method returns the version of the
record store in a particular directory.

=cut
sub detect_version {
    my( $pkg, $dir ) = @_;
    my $ver_file = "$dir/VERSION";
    my $source_version;
    if ( -e $ver_file ) {
        my $FH = _open( $ver_file, "<" );
        $source_version = <$FH>;
        chomp $source_version;
        close $FH;
    }
    return $source_version;
} #detect_version

# ---------------------- private stuffs -------------------------

sub _err {
    my ($method,$txt) = @_;
    print STDERR Data::Dumper->Dump([longmess]);
    die __PACKAGE__."::$method $txt";
}

sub _warn {
    my ($method,$txt) = @_;
    warn __PACKAGE__."::$method $txt";
}

sub _log {
    my ($self, $logstr) = @_;
    return if $self->[ARGS]{nolog};
#    print STDERR "$logstr\n";
    my $logfile = "$self->[0]/LOG";
    `touch $logfile`;
    open my $fh, '>>', $logfile;
    print $fh $logstr."\n";
}

sub _open {
    my ($file, $mode) = @_;
    my $fh;
    my $res = CORE::open ($fh, $mode, $file);
    if ($res) {
        $fh->blocking( 1 );
        return $fh;
    }
}

sub _stow {
    my ($self, $data, $id, $rs_override ) = @_;

    my $index = $self->[INDEX_SILO];

    if( defined $id && ($id < 1|| int($id) != $id)) {
        _err( 'stow', "id when supplied must be a positive integer" );
    }

    $index->ensure_entry_count( $id );

    my( $old_silo_id, $old_idx_in_silo, $old_creation_time );
    if( $id > 0 ) {
       ( $old_silo_id, $old_idx_in_silo, undef, $old_creation_time ) = @{$index->get_record($id)};
    }
    else {
        $id = $index->next_id;
    }

    my $data_write_size = do { use bytes; length $data };
    my $new_silo_id = _silo_id_for_size( $data_write_size, $self->[HEADER_SIZE], $self->[MIN_SILO_ID] );

    my $new_silo = $self->get_silo($new_silo_id);

    if (defined $old_silo_id && $new_silo_id == $old_silo_id) {
        my $t = _time();
        # new silo same as the old silo
        $new_silo->put_record( $old_idx_in_silo, [ $rs_override || RS_ACTIVE, $id, $data_write_size, $data ] );
        $index->put_record( $id, [$new_silo_id,$old_idx_in_silo, $t, $old_creation_time ? $old_creation_time : $t] );
        $self->_log( "$$ STOW \%$id ($data_write_size bytes) to $old_idx_in_silo/$new_silo_id $t" );
        return $old_silo_id;
    }

    my $new_idx_in_silo = $new_silo->push( [$rs_override || RS_ACTIVE, $id, $data_write_size, $data] );

    my $t = _time();

    $index->put_record( $id, [$new_silo_id,$new_idx_in_silo, $t, $old_creation_time ? $old_creation_time : $t] );

    $self->_log( "$$ STOW \%$id ($data_write_size bytes) to $new_idx_in_silo/$new_silo_id $t" );


    if( $old_silo_id ) {
#
# why could this not be marked as dead?
#
#$VAR1 = ' at lib/Yote/RecordStore/Silo.pm line 148.
# 	Yote::RecordStore::Silo::_put_record(Yote::RecordStore::Silo=ARRAY(0x55aa85b42b40), 21, ARRAY(0x55aa862d2868), "I") called at lib/Yote/RecordStore/Silo.pm line 143
# 	Yote::RecordStore::Silo::put_record(Yote::RecordStore::Silo=ARRAY(0x55aa85b42b40), 21, ARRAY(0x55aa862d2868), "I") called at lib/Yote/RecordStore.pm line 700
# 	Yote::RecordStore::_mark(Yote::RecordStore=ARRAY(0x55aa85e2d298), 14, 21, 2) called at lib/Yote/RecordStore.pm line 670
# 	Yote::RecordStore::_stow(Yote::RecordStore=ARRAY(0x55aa85e2d298), "\\x{d}\\x{0}\\x{0}\\x{0}GRU::IntArray\\x{d2}\\x{0}\\x{0}\\x{0}\\x{2}\\x{0}\\x{0}\\x{0}\\x{4}\\x{0}\\x{0}\\x{0}! \\x{1}\\x{0}datav\\x{a7}\\x{1}\\x{0}\\x{0}\\x{f7}\\x{5}\\x{0}\\x{0}~\\x{2}\\x{0}\\x{0}\\x{e2}\\x{2}\\x{0}\\x{0}\\x{9f}4\\x{0}\\x{0}!\\x{1}\\x{0}"..., 210) called at lib/Yote/RecordStore.pm line 366
# 	Yote::RecordStore::stow(Yote::RecordStore=ARRAY(0x55aa85e2d298), "\\x{d}\\x{0}\\x{0}\\x{0}GRU::IntArray\\x{d2}\\x{0}\\x{0}\\x{0}\\x{2}\\x{0}\\x{0}\\x{0}\\x{4}\\x{0}\\x{0}\\x{0}! \\x{1}\\x{0}datav\\x{a7}\\x{1}\\x{0}\\x{0}\\x{f7}\\x{5}\\x{0}\\x{0}~\\x{2}\\x{0}\\x{0}\\x{e2}\\x{2}\\x{0}\\x{0}\\x{9f}4\\x{0}\\x{0}!\\x{1}\\x{0}"..., 210) called at lib/Yote/ObjectStore.pm line 198
# 	Yote::ObjectStore::save(Yote::ObjectStore=ARRAY(0x55aa85fe78a0)) called at lib/GRU/Base.pm line 97
# 	GRU::Base::save(GRU::Populator=ARRAY(0x55aa85fe7b70), "saving exem 521") called at lib/GRU/Populator.pm line 206
# 	GRU::Populator::provide_base_pop(GRU::Populator=ARRAY(0x55aa85fe7b70), "./t/speed_test_files/2000_exems.txt") called at t/populator_speed_test.pl line 44
# 	main::run_base_async() called at t/populator_speed_test.pl line 37
# 	main::__ANON__ called at (eval 15) line 1
# 	Benchmark::__ANON__() called at /usr/share/perl/5.34/Benchmark.pm line 722
# 	Benchmark::runloop(3, CODE(0x55aa85e2a990)) called at /usr/share/perl/5.34/Benchmark.pm line 753
# 	Benchmark::timeit(3, CODE(0x55aa85e2a990)) called at /usr/share/perl/5.34/Benchmark.pm line 892
# 	Benchmark::timethis(3, CODE(0x55aa85e2a990), "async", "") called at /usr/share/perl/5.34/Benchmark.pm line 958
# 	Benchmark::timethese(3, HASH(0x55aa854d1578)) called at t/populator_speed_test.pl line 39
# ';
# Yote::RecordStore::Silo::put_record index 21 out of bounds for silo /tmp/jYvDQU4HgS/data_silos/14. Silo has entry count of 20 at lib/Yote/RecordStore/Silo.pm line 34.
#
        $self->_log( "$$ mark dead $old_idx_in_silo/$old_silo_id" );
        $self->_mark( $old_silo_id, $old_idx_in_silo, RS_DEAD );
    }

    return $id;
} #_stow

sub _fetch {
    my( $self, $id ) = @_;

    if( $id > $self->record_count ) {
        return undef;
    }
    my( $silo_id, $idx_in_silo, $update_time, $creation_time ) = @{$self->[INDEX_SILO]->get_record($id)};
    if( $silo_id ) {
        my $ret = $self->get_silo($silo_id)->get_record( $idx_in_silo );
        my $val = substr( $ret->[3], 0, $ret->[2] );
        my $data_read_size = do { use bytes; length $val };
        $self->_log( "$$ FETCH \%$id at ($idx_in_silo/$silo_id), ".length($data_read_size)." bytes" );
        return $update_time, $creation_time, $val;
    }
    return undef;
} #fetch

sub _mark {
    my ($self, $silo_id, $idx_in_silo, $rs ) = @_;
    $self->get_silo($silo_id)->put_record( $idx_in_silo, [ $rs ], 'I' );
}

sub _time {
    int(time * 1000);
}

sub _vacuum {
    my $self = shift;

    # scan each silo for stuff to vacuum
    my $silos = $self->[SILOS];
    for my $silo_id (0..$#$silos) {
        my $silo = $silos->[$silo_id];
        next unless $silo;

        my $start_id = 1;
        while ($start_id <= $silo->entry_count) {
            my( $rec_state, $id ) = (@{$silo->get_record( $start_id, 'I' )});
            if ($rec_state != RS_ACTIVE) {
                # potentially swap out to end
                $self->_vacate( $silo_id, $start_id );
            } else {
                $start_id++;
            }
        }
    }

} #_vacuum

sub _vacate {
    my( $self, $silo_id, $id_to_empty ) = @_;

    $self->_log( "$$ vacate $id_to_empty/$silo_id" );

    #
    # empty a data silo store entry.
    #
    # if this is at the end of the silo, just pop it off to reduce the silo size
    #
    # otherwise, move the last active entry to this cell, popping off inactive
    #            entries as you go
    #

    my $silo = $self->get_silo($silo_id);
    my $rc = $silo->entry_count;

    if( $id_to_empty == $rc ) {
        # if the one being vacated is the last one
        $silo->pop;
    }
    else {
        while( $rc > $id_to_empty ) {
            my( $rec_state, $id ) = (@{$silo->get_record( $rc, 'IL' )});
            if( $rec_state == RS_ACTIVE ) {
                # is active, so copy its data to the vacated position
                # and update the index
                $silo->copy_record($rc,$id_to_empty);
                # the following does not update the time field, it preserves it
                $self->[INDEX_SILO]->put_record( $id, [$silo_id,$id_to_empty], "IL" );
                $silo->pop;
                return 1;
            }
            # not an active record so pop it off and try again
            $silo->pop;
            $rc--;
        }
    }

    return 1;
} #_vacate

sub _size_for_silo_id {
    my ($silo_id) = @_;
    int ($file_quanta * $quanta_boost * $silo_id);
}

sub _silo_id_for_size {
    my( $data_write_size, $header_size, $min_silo_id ) = @_;
    
    my $write_size = $header_size + $data_write_size;
    
    my $silo_id = int($write_size / ($file_quanta*$quanta_boost));
#    print STDERR "$silo_id for $write_size < "._size_for_silo_id( $silo_id )."\n";
    $silo_id++ if _size_for_silo_id($silo_id) < $write_size;
    $silo_id = $min_silo_id if $silo_id < $min_silo_id;
    return $silo_id;
} #_silo_id_for_size

sub load_active_silos {
    my $self = shift;
    my $dir = "$self->[DIRECTORY]/data_silos/";
    # scan dir and open silos that correspond
    opendir( my $dh, $dir );
    no warnings 'numeric';
    map { $self->get_silo( $_ ) }
       grep {$_>0} 
       map { s!.*/(\d+)$!$1!; $_ } 
       readdir($dh);
}

sub get_silo {
    my ($self, $silo_id) = @_;
    my $silos = $self->[SILOS];
    my $silo = $silos->[$silo_id];
    if (!$silo) {
        $silo = $silos->[$silo_id] =
            $self->_open_silo( "$self->[DIRECTORY]/data_silos/$silo_id",
                               'ILLa*',
                               size => _size_for_silo_id( $silo_id ) );
    }
    return $silo;
}

# for testing monkeypatching
sub _open_silo {
    my ($self, $silo_file, $template, %args ) = @_;
    return Yote::RecordStore::Silo->open_silo( $silo_file,
                                               $template,
                                               %args );
}

sub _make_path {
    my( $dir, $err, $msg ) = @_;
    make_path( $dir, { error => $err } );
}

sub _reset {
    my $self = shift;

    $self->[TRANSACTION] = undef;
    my $index_silo = $self->index_silo;
    my $transaction_silo = $self->transaction_silo;
    my $silos = $self->silos;
    for my $silo ($index_silo, $transaction_silo, @$silos) {
        if ($silo) {
            $silo->sync_to_filesystem;
        }
    }
    $self->_fix_transactions;
}


sub _fix_transactions {
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
        my $trans = Yote::RecordStore::Transaction->open( $self, $last_trans );
        if ($trans) {
            $trans->fix;
        }
        $transaction_index_silo->pop;
        --$last_trans;
    }

} #_fix_transactions


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

Tries to detect the version of the Yote::RecordStore in
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
