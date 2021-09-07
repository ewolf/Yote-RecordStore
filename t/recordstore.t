#!/usr/bin/perl
use strict;
use warnings;
no warnings 'uninitialized';

use lib 't/lib';
use api;

use Data::Dumper;

use lib './lib';
use Yote::RecordStore::File;
use Yote::RecordStore::File::Silo;
use File::Path qw(make_path);
use Fcntl ':mode';
use File::Temp qw/ :mktemp tempdir /;
use File::Path qw/ remove_tree /;
use Test::More;
use Time::HiRes qw( usleep );
#use Errno qw(ENOENT);


use Carp;
$SIG{ __DIE__ } = sub { Carp::confess( @_ ) }; 

my $is_root = `whoami` =~ /root/;

# -----------------------------------------------------
#               init
# -----------------------------------------------------

my $factory = Factory->new;

warn "Need to test transactions";
#test_transactions();

test_use();

test_init();
#test_locks();
test_sillystrings();
test_meta();

api->test_failed_async( $factory );
#api->test_transaction_async( $factory );
#api->test_locks_async( $factory );
api->test_suite_recordstore( $factory );


done_testing;
exit;

sub failnice {
    my( $subr, $errm, $msg ) = @_;
    eval {
        $subr->();
        fail( $msg );
    };
    like( $@, qr/$errm/, "$msg error" );
    undef $@;
}

sub test_init {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore::File->open_store( $dir );

    ok( $rs, 'inited store' );
    is ( $rs->[Yote::RecordStore::File->MIN_SILO_ID], 12, "default min silo id" );
    is ( $rs->[Yote::RecordStore::File->MAX_SILO_ID], 31, "default max silo id" );

    my $silos = $rs->silos;
    is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 1 + (31 - 12), '20 silos' );

    $rs = Yote::RecordStore::File->open_store( $dir );
    ok( $rs, 'reopen store right stuff' );
    is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 1 + (31 - 12), 'still 20 silos' );

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store( "$dir/NOODIR" );

    ok( $rs, 'inited store' );

    $dir = tempdir( CLEANUP => 1 );

    {
        local $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE = 3_000_000_000;
        local $Yote::RecordStore::File::Silo::DEFAULT_MIN_FILE_SIZE = 300;
        $rs = Yote::RecordStore::File->open_store( $dir );
        ok( $rs, 'reinit store right stuff' );
        is ( $rs->[Yote::RecordStore::File->MIN_SILO_ID], 9, "min silo id for 300 min size" );
        is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 1 + (31 - 9), 'number of silos' );
    }

    {
        local $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE = 2 ** 12;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore::File->open_store( $dir );
        $silos = $rs->silos;
        is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 13 - 12, '1 silo' );
    }

    {
        local $Yote::RecordStore::File::Silo::DEFAULT_MIN_FILE_SIZE = 2 ** 10;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore::File->open_store($dir);
        is ( $rs->[Yote::RecordStore::File->MIN_SILO_ID], 10, 'min silo id is 10' );
        is ( $rs->[Yote::RecordStore::File->MAX_SILO_ID], 31, 'max silo id is 31' );
        $silos = $rs->silos;
        is ( $#$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 31 - 10, '21 silos' );
    }

    # if( ! $is_root ) {
    #     $dir = tempdir( CLEANUP => 1 );
    #     chmod 0444, $dir;
    #     failnice( sub { Yote::RecordStore::File->open_store("$dir/cant") },
    #               'permission denied',
    #               'made a directory that it could not' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     my $lockfile = "$dir/LOCK";
    #     open my $out, '>', $lockfile;
    #     print $out '';
    #     close $out;
    #     chmod 0444, $lockfile;
    #     failnice( sub { 
    #         Yote::RecordStore::File->open_store( $dir ) },
    #               "permission denied",
    #               "was able to init store with unwritable lock file" );

    #     $dir = tempdir( CLEANUP => 1 );
    #     Yote::RecordStore::File->open_store( $dir );
    #     chmod 0000, "$dir/LOCK";
    #     failnice( sub { Yote::RecordStore::File->open_store( $dir ) },
    #               'permission denied',
    #               'was able to reopen store with unwritable lock file' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     chmod 0444, "$dir";
    #     failnice( sub { Yote::RecordStore::File->open_store( $dir ) },
    #               'permission denied',
    #               'was not able to open store in unwritable directory' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     open $out, ">", "$dir/VERSION";
    #     print $out "666\n";
    #     close $out;
    #     failnice( sub { Yote::RecordStore::File->open_store( $dir ) },
    #               'Aborting open',
    #               'opened with version file but no lockfile' );
    # }

    {
        local $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE = 2 ** 12;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore::File->open_store( $dir );

        ok( $rs, 'opened a record store' );
        $silos = $rs->silos;
        is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 13 - 12, 'opened with correct number of silos' );
    }

} #test_init

sub test_locks {
    my $use_single = shift;
    my $dir = tempdir( CLEANUP => 1 );

    my $store = Yote::RecordStore::File->open_store( $dir );
    $store->lock( "FOO", "BAR", "BAZ", "BAZ" );

    eval {
        $store->lock( "SOMETHING" );
        fail( "Yote::RecordStore::File->lock called twice in a row" );
    };
    like( $@, qr/cannot be called twice in a row/, 'Yote::RecordStore::File->lock called twice in a row error message' );
    $store->unlock;
    $store->lock( "SOMETHING" );
    pass( "Store was able to lock after unlocking" );
    $store->unlock;

    $store->lock( "SOMETHING", "ZOMETHING" );
    pass( "Store was able to lock same label after unlocking" );
    $store->unlock;

    unless( $is_root ) {
        chmod 0444, "$dir/user_locks/SOMETHING";
        eval {
            $store->lock( "SOMETHING" );
            fail( "Store was able to lock unwritable lock file" );
        };
        like( $@, qr/lock failed/, 'lock failed when unwritable' );
        undef $@;
        chmod 0744, "$dir/user_locks/SOMETHING";

        chmod 0444, "$dir/user_locks/ZOMETHING";
        eval {
            $store->lock( "BUMTHING", "ZOMETHING" );
            fail( "Store was able to lock unwritable lock file" );
        };
        like( $@, qr/lock failed/, 'lock failed when unwritable' );
        undef $@;

        chmod 0744, "$dir/user_locks/ZOMETHING";
        $store->lock( "BUMTHING", "ZOMETHING" );
        pass( 'store able to lock with permissions restored' );
        $store->unlock;

        $dir = tempdir( CLEANUP => 1 );
        eval {
            $store = Yote::RecordStore::File->open_store( $dir );
            pass( "Was able to open store" );
            chmod 0444, "$dir/user_locks";

            $store->lock( "FOO" );
            fail( "Yote::RecordStore::File->lock didnt die trying to lock to unwriteable directory" );
        };
        like( $@, qr/lock failed/, "unable to lock because of unwriteable lock directory" );


        $dir = tempdir( CLEANUP => 1 );
        eval {
            $store = Yote::RecordStore::File->open_store( $dir );
            open my $out, '>', "$dir/user_locks/BAR";
            print $out '';
            close $out;
            chmod 0444, "$dir/user_locks/BAR";
            pass( "Was able to open store" );
            $store->lock( "FOO", "BAR", "BAZ" );
            fail( "Yote::RecordStore::File->lock didnt die trying to lock unwriteable lock file" );
        };
        like( $@, qr/lock failed/, "unable to lock because of unwriteable lock file" );

    }
} #test_locks

sub test_use {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore::File->open_store($dir);
    is ( $rs->record_count, 0, 'starts with no entry count' );
    my $id = $rs->stow( "FOOOOF" );
    is ( $rs->record_count, 1, 'added one thing' );
    is ( $rs->record_count, $id, 'id of one thing is that of entry count' );
    my $nid = $rs->next_id;
    is ( $rs->record_count, 2, 'added nuther thing' );
    $id = $rs->stow( "LOOOOL", $nid );
    is ( $rs->record_count, 2, 'stowed that nuther thing' );
    is ( $rs->index_silo->entry_count, 2, 'index silo also entry count 2' );
    is ( $nid, 2, 'second id by next' );
    is ( $id, $nid, 'entry count after forced set of things' );

    is ( $rs->fetch( 1 ), "FOOOOF", 'got first entry' );
    is ( $rs->fetch( 2 ), "LOOOOL", 'got second entry' );

    $rs->stow( "X"x10_000, 2 );
    is ( $rs->fetch( 2 ), "X"x10_000, 'got ten zousand xsx' );

    $rs->delete_record( 2 );

    is ( $rs->fetch( 2 ), undef, ' ten zousand xsx deletd' );
    $nid = $rs->next_id;
    is ( $rs->fetch( $nid ), undef, 'nothing when next id' );
    $rs->delete_record( $nid );

    is ( $rs->fetch( $nid ), undef, 'nothing when nothing next id dleted' );
    is ( $rs->next_id, 1 + $nid, 'still produces an other id after nothing delete' );

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store($dir);

    my $d1 = $rs->stow( "ZIPPO" );  #A 1
    $rs->stow( "BLINK" );           #B 2
    $rs->stow( "ZAP" );             #C 3
    my $d2 = $rs->stow( "XZOOR" );  #D 4
    my $d3 = $rs->stow( "ZSMOON" ); #E 5
    $rs->stow( "SOOZ" );            #F 6
    is ( $rs->record_count, 6, "6 records stowed before delete test" );
    is ( $rs->record_count, 6, "6 entries before delete test" );

    is ( $rs->silos_entry_count, 6, "6 items in silos" );
    is ( $rs->active_entry_count, 6, "6 active items in silos" );
    # A B C D E F
    $rs->delete_record( $d1 );
    # F B C D E
    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 5, "5 active items in silos after delete" );
    is ( $rs->silos_entry_count, 5, "now 5 items in silos after " );

    $rs->delete_record( $d2 );
    # F B C E
    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 4, "4 active items in silos after delete" );
    is ( $rs->silos_entry_count, 4, "now 4 items in silos after " );

    $rs->delete_record( $d3 );
    # F B C
    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 3, "3 active items in silos after delete" );
    is ( $rs->silos_entry_count, 3, "now 3 items in silos after " );

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store($dir);

    $rs->stow( "SOMETHING SOMETHING" ); #1
    $id = $rs->stow( "DELME LIKE" );    #2
    my $rs2 = Yote::RecordStore::File->open_store($dir);

    # $rs2->use_transaction;
    # $rs2->stow( "TO NOT BE MADE" );    #3
    
    # is ( $rs->record_count, 3, "3 entries in trans use" );
    # is ( $rs->active_entry_count, 2, "2 active entries in trans use as trans created block indexed in trans not in rs index" );
    # is ( $rs->silos_entry_count, 3, "3 items in silos in trans " );

    # $rs->delete_record( $id );
    
    # is ( $rs->record_count, 3, "3 entries in trans use after nontrans del" );
    # is ( $rs->active_entry_count, 1, "1 active entries in trans use as not yet committed" );
    # is ( $rs->silos_entry_count, 3, "3 items in silos in trans use because in transaction entry cant be moved to vacated spot" );
    
    # $rs2->rollback_transaction;
    
    # is ( $rs->record_count, 3, "3 entries after trans use" );
    # is ( $rs->active_entry_count, 1, "1 active entry after trans use" );
    # is ( $rs->silos_entry_count, 2, "2 items in silos in trans use because in transaction entry cant be moved to vacated spot" );
    
} #test_use

sub test_transactions {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore::File->open_store($dir);
    my $copy = Yote::RecordStore::File->open_store( $dir );
    my $id = $copy->stow( "SOMETHING ZOMETHING" );
    is ( $id, 1, "first id" );
    my $oid = $copy->stow( "OMETHING ELSE" );
    is ( $oid, 2, "second id" );

    is ($rs->rollback_transaction(), undef, 'cant roll back without transaction' );

    like( $@, qr/no transaction to roll back/i, "error message for trying to roll back no transaction" );

    is ($rs->commit_transaction(), undef,  "cant commit without transaction" );

    like( $@, qr/no transaction to commit/i, "error message for trying to commit no transaction" );

    is ( $rs->transaction_silo->entry_count, 0, 'trans silo starts with no count' );
    $copy->reset;
    is ( $copy->transaction_silo->entry_count, 0, 'copy trans silo starts with no count' );

    ok ($rs->use_transaction(), 'use transaction' );
    eval {
        local( *STDERR );
        my $errout;
        open( STDERR, ">>", \$errout );
        $rs->use_transaction();
        like( $errout, qr/already in transaction/, 'already in transaction warning' );
    };

    is ( $rs->transaction_silo->entry_count, 1, 'trans silo now with one count' );
    $copy->reset;
    is ( $copy->transaction_silo->entry_count, 1, 'copy trans silo now with one count' );

    eval {
        local( *STDERR );
        my $errout;
        open( STDERR, ">>", \$errout );

        $rs->use_transaction();
        like( $errout, qr/already in transaction/i, "error message for trying to transaction twice" );
        pass( "able to open transaction twice" );
    };

    is ( $rs->transaction_silo->entry_count, 1, 'trans silo still with one count' );
    is ( $copy->transaction_silo->entry_count, 1, 'copy trans silo still with one count' );
    is ( $rs->fetch( $oid ), "OMETHING ELSE", 'trans has right val for something not in trans' );
    $rs->stow( "THIS MEANS SOMETHING", $id );
    $copy->reset;

    is ( $rs->fetch( $id ), "THIS MEANS SOMETHING", "correct value in transaction" );
    is ( $copy->fetch( $id ), "SOMETHING ZOMETHING", "correct value in non-transaction" );

    my $newid = $rs->stow( "ONE MORE THING TO STOWWWWWWWW" );
    is ( $newid, 3, '3rd id' );
    is ( $rs->fetch( $newid ), "ONE MORE THING TO STOWWWWWWWW", "correct value assigned id" );
    is ( $copy->fetch( $newid ), undef, "copy cant see commit value" );

    $rs->stow( "OH LETS CHANGE THIS UP", $newid );
    $copy->reset;

    is ( $rs->fetch( $newid ), "OH LETS CHANGE THIS UP", "correct new value assigned id" );
    is ( $copy->fetch( $newid ), undef, "copy still cant see commit value" );

    my $nid = $rs->stow( "FORGRABS" );
    $copy->reset;
    my $nextid = $copy->next_id;
    is ( $nextid, 1 + $nid, 'copy gets one id further than trans store' );
    is ( $rs->fetch( $nid ), "FORGRABS", "correct new value assigned id" );
    is ( $copy->fetch( $nid ), undef, "copy still cant see commit value" );

    $rs->delete_record( $nid );
    is ( $rs->fetch( $nid ), undef, "correct deleted assigned id" );
    is ( $copy->fetch( $nid ), undef, "copy still cant see commit value" );

    $copy->stow( "neet", $nextid );
    $rs->reset;
    is ( $rs->fetch( $nextid ), 'neet', "trans sees copy stow" );
    is ( $copy->fetch( $nextid ), 'neet', "copy sees own stow" );

    $rs->delete_record( $nextid );
    is ( $rs->fetch( $nextid ), undef, "trans cant see deleted item" );
    is ( $copy->fetch( $nextid ), 'neet', "copy still sees own stow" );

    $rs->stow( "X" x 10_000 );

    $rs->commit_transaction();
    $copy->reset;
    is ( $copy->fetch( $id ), "THIS MEANS SOMETHING", "correct value in copy after commit" );
    is ( $rs->fetch( $id ), "THIS MEANS SOMETHING", "correct value in after commit" );

    is ( $rs->fetch( $nextid ), undef, "trans cant see deleted item after commit" );
    is ( $copy->fetch( $nextid ), undef, "copy sees deleted own stow after commit" );

    is ( $copy->fetch( $nextid + 1 ), "X" x 10_000, 'big thing saved' );

    if( ! $is_root ) {
        $dir = tempdir( CLEANUP => 1 );
        my $stacks = "$dir/transactions/1";
        make_path( $stacks );
        chmod 0444, $stacks;
        $rs = Yote::RecordStore::File->open_store($dir);
        eval {
            $rs->use_transaction;
            fail( 'was able to use transaction with unwritable dir' );
        };
        like( $@, qr/permission denied/i, 'err msg for trans with unwrite dir' );
    }

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store($dir);
    $copy = Yote::RecordStore::File->open_store( $dir );
    
    $rs->use_transaction;
    
    $newid = $rs->stow( "WIBBLES AND FOOKZA" );
    $id = $copy->stow( "SOMETHING TO NEARLY DELETE" );
    $rs->reset;
    $rs->delete_record( $id );

    is ( $rs->fetch( $id ), undef, "trans cant see deleted item before commit" );
    is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );

    $rs->stow( "BLAME ME", $id );

    is ( $rs->fetch( $id ), "BLAME ME", "trans sees updated before commit" );
    is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );

    $rs->delete_record( $id );

    $rs->delete_record( $id );

    is ( $rs->fetch( $id ), undef, "trans cant see deleted item before commit" );
    is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );
    
    is ( $rs->fetch( $newid ), "WIBBLES AND FOOKZA", "trans can see stowed item before rollback" );
    is ( $copy->fetch( $newid ), undef, "copy cant see stowed item before rollback" );

    $rs->rollback_transaction;

    is ( $rs->fetch( $id ), "SOMETHING TO NEARLY DELETE", "rs sees thing after transaction delete rollback" );
    is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );
    is ( $rs->fetch( $newid ), undef, "trans cant see stowed item after rollback" );
    is ( $copy->fetch( $newid ), undef, "copy cant see stowed item after rollback" );

    
    # need to monkey patch to test rollback of partially commited thing
    
    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store($dir);
    $copy = Yote::RecordStore::File->open_store( $dir );

    my $times = 100_000;
    
    my $id1 = $copy->stow( "THIS IS THE FIRST ONE IN THE FIRST PLACE" );
    my $id2 = $copy->stow( "DELETE ME OR TRY TO" );
    my $id3 = $copy->stow( "OH, CHANGE THIS REALLY BIG THING"x$times );
    my $id4 = $copy->stow( "CHANGE ME UP" );
    $rs->reset;
    my( $breakid );

    $rs->use_transaction;
    
    $rs->stow( "GROWING IT UP"x$times, $id1 );
    $rs->delete_record( $id2 );

    $rs->stow( "SHRINKING THIS DOWN", $id3 );
    $rs->stow( "CHANGED THIS UP", $id4 );
    
    $breakid = $rs->stow( "BREAK ON THIS" );

    no strict 'refs';
    use Fcntl qw( :flock SEEK_SET );
    local *Yote::RecordStore::File::Silo::put_record = sub {
        my( $self, $id, $data, $template, $offset ) = @_;
        if( $self->[Yote::RecordStore::File::Silo->CUR_COUNT+1] ) {
            if( $id == $breakid ) {
                $rs->unlock;
                $@ = "Breakpoint";
                return undef;
            }
        }

        if( $id > $self->entry_count || $id < 1 ) {
            $@ = "Yote::RecordStore::File::Silo->put_record : index $id out of bounds for silo $self->[0]. Store has entry count of ".$self->entry_count;
            return undef;
        }
        if( ! $template ) {
            $template = $self->[Yote::RecordStore::File::Silo->TEMPLATE];
        }

        my $rec_size = $self->[Yote::RecordStore::File::Silo->RECORD_SIZE];
        my $to_write = pack ( $template, ref $data ? @$data : $data );
        # allows the put_record to grow the data store by no more than one entry
        my $write_size = do { use bytes; length( $to_write ) };
        if( $write_size > $rec_size) {
            $@ = "Yote::RecordStore::File::Silo->put_record : record size $write_size too large. Max is $rec_size";
            return undef;
        }

        my( $idx_in_f, $fh, $subsilo_idx ) = $self->_fh( $id );

        $offset //= 0;
        my $seek_pos = $rec_size * $idx_in_f + $offset;
        sysseek( $fh, $seek_pos, SEEK_SET );
        syswrite( $fh, $to_write );

        return 1;
    };
    use strict 'refs';


    $rs->index_silo->[Yote::RecordStore::File::Silo->CUR_COUNT+1] = 1;

    is ( $rs->fetch( $id1 ), "GROWING IT UP"x$times, "in trans with correct value" );
    is ( $rs->fetch( $id2 ), undef, "deleted in trans" );
    is ( $rs->fetch( $id3 ), "SHRINKING THIS DOWN", "other changed val in trans");
    is ( $rs->fetch( $id4 ), "CHANGED THIS UP", "~ same size thing changed in trans");
    is ($rs->commit_transaction, undef,  "cant commit due to monkeypatched error" );
    like ($@, qr/Breakpoint/, 'break msg' );

    is ( $rs->use_transaction->{state}, Yote::RecordStore::File::Transaction::TR_IN_COMMIT, "transaction is in commit" );
    is ( $rs->fetch( $id1 ), undef, 'fetch returns undef when in bad state' );
    like ($@, qr/Transaction is in a bad state/ );

    # test if the state of the recordstore fixes itself with transaction in bad state
    is ( $copy->fetch( $id1 ), "THIS IS THE FIRST ONE IN THE FIRST PLACE", "id 1 unchanged" );
    is ( $copy->fetch( $id2 ), "DELETE ME OR TRY TO", "id 2 unchanged" );
    is ( $copy->fetch( $id3 ), "OH, CHANGE THIS REALLY BIG THING"x$times, "id 3 unchanged" );
    is ( $copy->fetch( $id4 ), "CHANGE ME UP", "id 4 unchanged" );
        
    use strict 'refs';

    $rs->index_silo->[Yote::RecordStore::File::Silo->CUR_COUNT+1] = 1;

    is ( $rs->fetch( $id1 ), "NEW STOW AND MAYBE IT WILL GO"x$times, "in trans with correct value." );
    is ( $rs->fetch( $id2 ), undef, "deleted in trans" );
    is ( $rs->fetch( $id3 ), "CHANGING THIS SOME", "other changed val in trans");

    eval {
        $rs->rollback_transaction;
        fail( "able to rollback without the monkeypatched die" );
    };
    like ($@, qr/Breakpoint/, 'break msg' );
    eval {
        my $res = $rs->fetch( $id1 );
        fail( "Able to get result from bad transaction" );
    };
    if( $@ ) {
        like ($@, qr/Transaction is in a bad state/, 'bad state errm' );
    }
    # test if the state of the recordstore fixes itself with transaction in bad state
    is ( $copy->fetch( $id1 ), "THIS IS THE FIRST ONE IN THE FIRST PLACE", "id 1 unchanged" );
    is ( $copy->fetch( $id2 ), "DELETE ME OR TRY TO", "id 2 unchanged" );
    is ( $copy->fetch( $id3 ), "OH, CHANGE THIS REALLY BIG THING"x$times, "id 3 unchanged" );

    $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );
    $id = $store->stow( "FOO" );
    $store->stow( "BAR" );
    $id2 = $store->stow( "DELME" );
    $store->use_transaction;
    $store->delete_record( $id2 );
    is ( $store->silos_entry_count, 3, '3 entries in silos with commit with last delete' );
    $store->commit_transaction;
    is ( $store->silos_entry_count, 2, '2 entries remain in silos after commit w last delete' );
    $store->delete_record( $id );
    is ( $store->silos_entry_count, 1, 'now just one entry after commit with last delete' );

    $dir = tempdir( CLEANUP => 1 );
    $store = Yote::RecordStore::File->open_store( $dir );
    $id = $store->stow( "FOO" );
    $store->stow( "BAR" );
    $id2 = $store->stow( "DELME" );

    $store = Yote::RecordStore::File->open_store( $dir );
    $store->use_transaction;
    $store->delete_record( $id2 );
    is ( $store->silos_entry_count, 3, '3 entries in silos with commit with last delete' );

    no strict 'refs';

    local *Yote::RecordStore::File::_vacate = sub {
        my( $self, $silo_id, $id_to_empty ) = @_;
        print STDERR "VAC\n";
        if ( $silo_id == 12 && $id_to_empty == $id2 ) {
            $self->_unlock;
            die "VACATE BREAK";
        }
        my $silo = $self->[Yote::RecordStore::File->SILOS][$silo_id];
        my $rc = $silo->entry_count;
        if ( $id_to_empty == $rc ) {
            $silo->pop;
        } else {
            while ( $rc > $id_to_empty ) {
                my( $state, $id ) = (@{$silo->get_record( $rc, 'IL' )});
                if ( $state == Yote::RecordStore::File->RS_ACTIVE ) {
                    $silo->copy_record($rc,$id_to_empty);
                    $self->[Yote::RecordStore::File->INDEX_SILO]->put_record( $id, [$silo_id,$id_to_empty], "IL" );
                    $silo->pop;
                    return;
                } elsif ( $state == Yote::RecordStore::File->RS_DEAD ) {
                    $silo->pop;
                } else {
                    return;
                }
                $rc--;
            }
        }
    };
    no strict 'refs';
    failnice( sub { $store->commit_transaction },
              'VACATE BREAK',
              "was able to commit without breakage" );

    $store = Yote::RecordStore::File->open_store( $dir );
    is ( $store->silos_entry_count, 3, 'still 3 entries in silos after commit' );

    $store->fetch( $id, 'FOO', 'reopen store still foos' );
    is ( $store->silos_entry_count, 3, 'foo doesnt fix the interrupted commit because the interrupted commit was marked complete and just had a few things not purged from the record store ' );
    $store->delete_record( $id );
    is ( $store->silos_entry_count, 1, 'back down to bar' );

} #test_transactions


sub test_big {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );
    for( 1 .. 15 ) {
        my $beegstring = "BIIIIIG" x 10_000_000;
        my $id = $store->stow( $beegstring );
        is ( $store->fetch( $id ), $beegstring, "Big $_" );
    }
}


sub test_sillystrings {

    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );
    my $packed = pack( "I*", (0..100) );
    my $id = $store->stow( $packed );
    is ( $store->fetch($id), $packed, "packed string worked" );

} #test_sillystrings


sub test_meta {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );
    my $id = $store->stow( "THISISATEST" );

    my ($upd, $cr ) = $store->fetch_meta( $id );
    ok ($upd > 0, 'has a last updated time' );
    ok ($cr > 0, 'has a created time' );
    usleep( 1000 );
    $store->stow( "BLLBLBBL", $id );

    my ($upd2, $cr2 ) = $store->fetch_meta( $id );

    ok ($upd2 > $upd, 'updated time is greater' );
    is ($cr2, $cr, 'created time did not change' );
    
    
}

package Factory;

use File::Temp qw/ :mktemp tempdir /;

sub new { return bless {}, shift }

sub new_rs {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store($dir);

    return $store;
}
sub reopen {
    my( $cls, $oldstore ) = @_;
    return Yote::RecordStore::File->open_store( $oldstore->[Yote::RecordStore::File->DIRECTORY] );
}

__END__
