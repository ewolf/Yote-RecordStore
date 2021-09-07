#!/usr/bin/perl
use strict;
use warnings;
no warnings 'uninitialized';

use lib 't/lib';
use api;

use Data::Dumper;

use lib './lib';
use Yote::RecordStore;
use Yote::RecordStore::Silo;
use File::Path qw(make_path);
use Fcntl qw(:mode :flock SEEK_SET);
use File::Temp qw/ :mktemp tempfile tempdir /;
use File::Path qw/ remove_tree /;
use Scalar::Util qw(openhandle);
use Test::More;
#use Errno qw(ENOENT);


use Carp;
#$SIG{ __DIE__ } = sub { Carp::confess( @_ ) };

my $is_root = `whoami` =~ /root/;

# -----------------------------------------------------
#               init
# -----------------------------------------------------

my $factory = Factory->new;

test_init();
test_use();

test_transactions();

#test_locks();

test_sillystrings();
test_meta();

test_vacate();

test_misc();

#api->test_failed_async( $factory );
#api->test_transaction_async( $factory );
#api->test_locks_async( $factory );
#api->test_suite_recordstore( $factory );


done_testing;
exit;

sub failnice {
    my( $subr, $errm, $msg ) = @_;
    local( *STDERR );
    my $errout;
    open( STDERR, ">>", \$errout );
    eval {
        $subr->();
        fail( "$msg fail" );
    };
    like( $@, qr/$errm/, "$msg error" );

    undef $@;
}

sub warnnice {
    my( $subr, $val, $errm, $msg ) = @_;
    local( *STDERR );
    my $errout;
    open( STDERR, ">>", \$errout );
    is ($subr->(), $val, "$msg value" );
    like( $errout, qr/$errm/, "$msg error" );
}

sub noSTDERR {
    my( $subr ) = @_;
    local( *STDERR );
    my $errout;
    open( STDERR, ">>", \$errout );
    $subr->();
}


sub get_rec {
    my( $id, $silo, $limit) = @_;
    my $res = $silo->get_record($id);
    if (@$res > 3) {
        $res->[3] = substr( $res->[3], 0, $res->[2] );
    }
    if ($limit) {
        return [@$res[0..($limit-1)]];
    }
    $res;
}

sub test_misc {
    my $dir = tempdir( CLEANUP => 1 );
    chmod 0444, $dir;
    failnice( sub{Yote::RecordStore::_open( "$dir/foo" )},
              'Permission denied',
              'open write only file' );
    chmod 0666, $dir;
}

sub test_vacate {
    my $dir = tempdir( CLEANUP => 1 );

    my $rs = Yote::RecordStore->open_store( $dir );

    my $silo = $rs->silos->[12];
    is ($silo->entry_count, 0, 'silo starts off empty' );

    locks ($rs);

    my $id = $rs->stow( "ZERO" );

    is ($silo->entry_count, 1, 'silo now has one' );

    ok ($rs->use_transaction);

    $rs->stow( "ONE" );
    $rs->stow( "TWO" );
    $rs->stow( "THREE" );

    # force fake unlock
    my $trans = $rs->[$rs->TRANSACTION];
    $rs->[$rs->TRANSACTION] = undef;

    $id = $rs->stow( "GOING" );

    is ($silo->entry_count, 5, 'silo with 5 items' );

    is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,4,'ZERO'], 'rec 1' );
    is_deeply( get_rec(2, $silo), [$rs->RS_IN_TRANSACTION,2,3,'ONE'], 'rec 2' );
    is_deeply( get_rec(3, $silo), [$rs->RS_IN_TRANSACTION,3,3,'TWO'], 'rec 3' );
    is_deeply( get_rec(4, $silo), [$rs->RS_IN_TRANSACTION,4,5,'THREE'], 'rec 4' );
    is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,5,'GOING'], 'rec 5' );

    # simulate transaction saved but not carried out
    ok ($trans->_save, 'trans could save');
    is ($silo->entry_count, 6, 'silo with 6 items after trans save' );

    # silo with [ ZERO, xx, xx, xx, GOING ]
    # with vacate should go to
    # silo with [ ZERO, GOING ]
    #$rs->_vacate( 12, 2 );
    is ($trans->rollback, 1, 'rolled back' );

    $silo->reset;

    is ($rs->transaction_silo->entry_count, 0, 'no entries in trans silo post rollback' );

    {
        # test an interrupted stow with items marked RS_IN_TRANSACTION
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );

        locks ($store);

        $store->stow( "A" );

        $store->use_transaction;

        $store->stow( "TB" );
        $store->stow( "TC" );
        $store->stow( "TD" );

        $store->[$store->TRANSACTION] = undef;

        my $silo = $store->silos->[12];

        # now it should be at a state like so:
        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,1,'A'], 'rec 1' );
        is_deeply( get_rec(2, $silo), [$rs->RS_IN_TRANSACTION,2,2,'TB'], 'rec 2' );
        is_deeply( get_rec(3, $silo), [$rs->RS_IN_TRANSACTION,3,2,'TC'], 'rec 3' );
        is_deeply( get_rec(4, $silo), [$rs->RS_IN_TRANSACTION,4,2,'TD'], 'rec 4' );
        is ($silo->entry_count, 4, '4 silo entries' );

    }

    {
        # test an interrupted stow with items marked RS_IN_TRANSACTION
        $dir = tempdir( CLEANUP => 1 );

        my $store = Yote::RecordStore->open_store( $dir );

        locks ($store);

        $store->stow( "A" );

        $store->use_transaction;

        $store->stow( "TB" );
        $store->stow( "TC" );
        $store->stow( "TD" );

        ok ($store->commit_transaction, 'could commit trans' );
        is ($store->transaction_silo->entry_count, 0, 'no entries in trans silo post commit' );

        my $silo = $store->silos->[12];

        # now it should be at a state like so:
        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,1,'A'], 'rec 1' );
        is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,2,2,'TB'], 'rec 2' );
        is_deeply( get_rec(3, $silo), [$rs->RS_ACTIVE,3,2,'TC'], 'rec 3' );
        is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,2,'TD'], 'rec 4' );

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::Silo::copy_record = sub {
            die 'monkey wrench';
        };

        failnice (sub {$store->_vacate( 12, 2 )},
                  'monkey wrench',
                  'vacate failed because of entries in transaction');
        is ($silo->entry_count, 4, 'still 4 silo entries' );
    }

    {
        # test a transaction that gets to the marked completed stage, but doesn't do the cleanup steps
        # after that
        $dir = tempdir( CLEANUP => 1 );


        my $store = Yote::RecordStore->open_store( $dir );

        my $silo = $store->silos->[12];
        my $bsilo = $store->silos->[13];
        my $isilo = $store->index_silo;
        my $tsilo = $store->transaction_silo;

        my $large = "X" x 7_000;
        {
            locks ($store);

            ok ($store->use_transaction, 'use trans' );

            is ($tsilo->entry_count, 1, 'one trans' );

            $store->stow( $large );

            is ($silo->entry_count, 0, 'nothing in small silo' );
            is ($bsilo->entry_count, 1, 'one big store' );

            no strict 'refs';
            no warnings 'redefine';
            local *Yote::RecordStore::Silo::pop = sub {
                my $self = shift;
                if ($self->[0] =~ /trans/) {
                    die 'monkey wrench';
                }
                $self->_pop;
            };
            failnice(sub{$store->commit_transaction}, 'monkey wrench', 'commit failed at pop');

            # must do this because the monkeypatching seems to mess with the
            # garbage collection
            $store->[$store->TRANSACTION] = undef;
            unlocks ( $store );

            # check the state, shoud have one competed transaction
            is ( $bsilo->entry_count, 1, 'only one thing in big silo' );
            is ( $silo->entry_count, 1, 'only one uncleaned up thing in silo' );
            is_deeply( get_rec(1, $bsilo), [$rs->RS_ACTIVE,1,7_000,$large], 'rec 1' );
            is_deeply( get_rec(1, $silo), [$rs->RS_DEAD,2, 24, pack ("IIIIII", $store->RS_ACTIVE,1,0,0,13,1 )], 'dead trans obj' );

        }

        $store = Yote::RecordStore->open_store( $dir );
        is_deeply( get_rec(1, $bsilo), [$rs->RS_ACTIVE,1,7_000,$large], 'rec 1' );
        $silo = $store->silos->[12];
        $bsilo = $store->silos->[13];
        $isilo = $store->index_silo;
        $tsilo = $store->transaction_silo;

        is ( $bsilo->entry_count, 1, 'only one thing in big silo' );
        is ( $silo->entry_count, 0, 'things cleaned up in silo' );

        is_deeply( get_rec(1, $bsilo), [$rs->RS_ACTIVE,1,7_000,$large], 'rec 1' );
    }

}

sub test_init {
    my $dir = tempdir( CLEANUP => 1 );

    is (Yote::RecordStore->first_id, 1, 'first id');

    is (Yote::RecordStore->detect_version( $dir ), undef, 'no version file yet');

    my $rs = Yote::RecordStore->open_store( $dir );

    ok (Yote::RecordStore->detect_version( $dir ) > 0, 'version file has version');
    is (Yote::RecordStore->detect_version( $dir ), Yote::RecordStore->VERSION, 'version file with correct version');

    ok( $rs, 'inited store' );
    is ($rs->directory, $dir, "recordstore directory" );

    is ( $rs->[Yote::RecordStore->MIN_SILO_ID], 12, "default min silo id" );
    is ( $rs->[Yote::RecordStore->MAX_SILO_ID], 31, "default max silo id" );

    my $silos = $rs->silos;
    is ( @$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 1 + (31 - 12), '20 silos' );
    $rs->[$rs->LOCK_FH] = undef;

    $rs = Yote::RecordStore->open_store( $dir );
    ok( $rs, 'reopen store right stuff' );
    is ( @$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 1 + (31 - 12), 'still 20 silos' );

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore->open_store( "$dir/NOODIR" );

    ok( $rs, 'inited store' );

    $dir = tempdir( CLEANUP => 1 );

    {
        local $Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE = 3_000_000_000;
        local $Yote::RecordStore::Silo::DEFAULT_MIN_FILE_SIZE = 300;
        $rs = Yote::RecordStore->open_store( $dir );
        ok( $rs, 'reinit store right stuff' );
        is ( $rs->[Yote::RecordStore->MIN_SILO_ID], 9, "min silo id for 300 min size" );
        is ( @$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 1 + (31 - 9), 'number of silos' );
    }

    {
        local $Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE = 2 ** 12;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore->open_store( $dir );
        $silos = $rs->silos;
        is ( @$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 13 - 12, '1 silo' );
    }

    {
        local $Yote::RecordStore::Silo::DEFAULT_MIN_FILE_SIZE = 2 ** 10;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore->open_store($dir);
        is ( $rs->[Yote::RecordStore->MIN_SILO_ID], 10, 'min silo id is 10' );
        is ( $rs->[Yote::RecordStore->MAX_SILO_ID], 31, 'max silo id is 31' );
        $silos = $rs->silos;
        is ( $#$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 31 - 10, '21 silos' );
    }

    # if( ! $is_root ) {
    #     $dir = tempdir( CLEANUP => 1 );
    #     chmod 0444, $dir;
    #     failnice( Yote::RecordStore->open_store("$dir/cant"),
    #               'permission denied',
    #               'made a directory that it could not' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     my $lockfile = "$dir/LOCK";
    #     open my $out, '>', $lockfile;
    #     print $out '';
    #     close $out;
    #     chmod 0444, $lockfile;
    #     failnice(
    #         Yote::RecordStore->open_store( $dir ),
    #               "permission denied",
    #               "was able to init store with unwritable lock file" );

    #     $dir = tempdir( CLEANUP => 1 );
    #     Yote::RecordStore->open_store( $dir );
    #     chmod 0000, "$dir/LOCK";
    #     failnice( Yote::RecordStore->open_store( $dir ),
    #               'permission denied',
    #               'was able to reopen store with unwritable lock file' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     chmod 0444, "$dir";
    #     failnice( Yote::RecordStore->open_store( $dir ),
    #               'permission denied',
    #               'was not able to open store in unwritable directory' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     open $out, ">", "$dir/VERSION";
    #     print $out "666\n";
    #     close $out;
    #     failnice( Yote::RecordStore->open_store( $dir ),
    #               'Aborting open',
    #               'opened with version file but no lockfile' );
    # }

    {
        local $Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE = 2 ** 12;
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore->open_store( $dir );

        ok( $rs, 'opened a record store' );
        $silos = $rs->silos;
        is ( @$silos - $rs->[Yote::RecordStore->MIN_SILO_ID], 13 - 12, 'opened with correct number of silos' );
    }

} #test_init

sub test_locks {
    my $use_single = shift;
    my $dir = tempdir( CLEANUP => 1 );

    my $store = Yote::RecordStore->open_store( $dir );
    $store->lock( "FOO", "BAR", "BAZ", "BAZ" );

    eval {
        $store->lock( "SOMETHING" );
        fail( "Yote::RecordStore->lock called twice in a row" );
    };
    like( $@, qr/cannot be called twice in a row/, 'Yote::RecordStore->lock called twice in a row error message' );
    undef $@;
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
            $store = Yote::RecordStore->open_store( $dir );
            pass( "Was able to open store" );
            chmod 0444, "$dir/user_locks";

            $store->lock( "FOO" );
            fail( "Yote::RecordStore->lock didnt die trying to lock to unwriteable directory" );
        };
        like( $@, qr/lock failed/, "unable to lock because of unwriteable lock directory" );
        undef $@;

        $dir = tempdir( CLEANUP => 1 );
        eval {
            $store = Yote::RecordStore->open_store( $dir );
            open my $out, '>', "$dir/user_locks/BAR";
            print $out '';
            close $out;
            chmod 0444, "$dir/user_locks/BAR";
            pass( "Was able to open store" );
            $store->lock( "FOO", "BAR", "BAZ" );
            fail( "Yote::RecordStore->lock didnt die trying to lock unwriteable lock file" );
        };
        like( $@, qr/lock failed/, "unable to lock because of unwriteable lock file" );
        undef $@;
    }
} #test_locks

sub test_use {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore->open_store($dir);

    failnice (sub {$rs->record_count},
               'store not locked',
               'cant use record_count until store is locked' );

    failnice (sub{$rs->active_entry_count},
               'store not locked',
               'cant use active_entry_count until store is locked' );

    failnice (sub{$rs->stow( "THE FIRST" )},
               'store not locked',
               'cant use stow until store is locked' );

    failnice (sub{$rs->fetch(1)},
               'store not locked',
               'cant use fetch until store is locked' );

    failnice (sub{$rs->fetch_meta(1)},
               'store not locked',
               'cant use fetch_meta until store is locked' );

    warnnice (sub{$rs->unlock(1)},
              1,
              'store not locked',
              'cant use unlock until store is locked' );

    failnice (sub{$rs->next_id},
               'store not locked',
               'cant use next_id until store is locked' );

    failnice (sub{$rs->delete_record(12)},
               'store not locked',
               'cant use delete_record until store is locked' );

    failnice (sub{$rs->silos_entry_count},
               'store not locked',
               'cant use silos_entry_count until store is locked' );


    failnice (sub{$rs->use_transaction},
               'store not locked',
               'cant use use_transaction until store is locked' );

    failnice (sub{$rs->commit_transaction},
               'store not locked',
               'cant use commit_transaction until store is locked' );

    failnice (sub{$rs->rollback_transaction},
               'store not locked',
               'cant use rollback_transaction until store is locked' );


    ok ( ! $rs->is_locked, 'not locked' );

    is ($@, undef, 'no error after lock check');


    ok ($rs->lock, 'able to lock record store');
    is ( $rs->is_locked, 1, 'now locked' );

    failnice (sub{$rs->rollback_transaction},
               'no transaction',
               'cant use rollback_transaction until store is locked' );


    failnice (sub{$rs->delete_record(12)},
               'past end of',
               'cant use delete_record until store is locked' );

    failnice (sub{$rs->stow("GAWOOOUUNGA", 0)},
               'must be a positive integer',
               'cant use delete_record until store is locked' );

    failnice (sub{$rs->stow("GAWOOOUUNGA", 1.5)},
               'must be a positive integer',
               'cant use delete_record until store is locked' );

    failnice (sub{$rs->stow("GAWOOOUUNGA", -2)},
               'must be a positive integer',
               'cant use delete_record until store is locked' );


    is ($rs->record_count, 0, 'starts with no entry count' );
    my $id = $rs->stow( "FOOOOF" );
    is ($rs->record_count, 1, 'added one thing' );
    is ($rs->record_count, $id, 'id of one thing is that of entry count' );
    my $nid = $rs->next_id;
    is ( $rs->record_count, 2, 'added nuther thing' );
    $id = $rs->stow( "LOOOOL", $nid );
    is ( $rs->record_count, 2, 'stowed that nuther thing' );
    is ( $rs->index_silo->entry_count, 2, 'index silo also entry count 2' );
    is ( $nid, 2, 'second id by next' );
    is ( $id, $nid, 'entry count after forced set of things' );

    is ( $rs->fetch( 1 ), "FOOOOF", 'got first entry' );
    is ( $rs->fetch( 2 ), "LOOOOL", 'got second entry' );
    warnnice (sub{$rs->fetch( 22 )},
              undef,
               'past end of records',
               'fetch past end of records returns undef' );

    $rs->stow( "X"x10_000, 2 );
    is ( $rs->fetch( 2 ), "X"x10_000, 'got ten zousand xsx' );

    $rs->delete_record( 2 );

    is ( $rs->fetch( 2 ), undef, ' ten zousand xsx deletd' );
    $nid = $rs->next_id;
    is ( $rs->fetch( $nid ), undef, 'nothing when next id' );
    $rs->delete_record( $nid );

    is ( $rs->fetch( $nid ), undef, 'nothing when nothing next id dleted' );
    is ( $rs->next_id, 1 + $nid, 'still produces an other id after nothing delete' );

    # ---------------------

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore->open_store($dir);
    $rs->lock;

    is ($@, undef, 'no error after rs lock');

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


    my $silo = $rs->silos->[12];

    is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,5,'ZIPPO'], 'rec 1' );
    is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,2,5,'BLINK'], 'rec 2' );
    is_deeply( get_rec(3, $silo), [$rs->RS_ACTIVE,3,3,'ZAP'], 'rec 3' );
    is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,5,'XZOOR'], 'rec 4' );
    is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,6,'ZSMOON'], 'rec 5' );
    is_deeply( get_rec(6, $silo), [$rs->RS_ACTIVE,6,4,'SOOZ'], 'rec 6' );


    # A B C D E F

    $rs->delete_record( $d1 );

    # F B C D E
    is_deeply( get_rec(1, $silo), [$rs->RS_DEAD,1,5,'ZIPPO'], 'rec 1' );
    is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,2,5,'BLINK'], 'rec 2' );
    is_deeply( get_rec(3, $silo), [$rs->RS_ACTIVE,3,3,'ZAP'], 'rec 3' );
    is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,5,'XZOOR'], 'rec 4' );
    is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,6,'ZSMOON'], 'rec 5' );

    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 5, "5 active items in silos after delete" );
    $rs->delete_record( $d2 );

    # F B C E
    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 4, "4 active items in silos after delete" );
    $rs->delete_record( $d3 );
    # F B C
    is ( $rs->record_count, 6, "still 6 entires after deleting penultimate" );
    is ( $rs->active_entry_count, 3, "3 active items in silos after delete" );

    ok ($rs->lock, "got lock");
    is (flock( $rs->[Yote::RecordStore->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh' );
    ok ($rs->unlock, "unlock");

    {
        my $trydir = tempdir( CLEANUP => 1 ) . '/addy';
        my $breakon = 'base';
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::_make_path = sub {
            my ( $dir, $err, $msg ) = @_;
            if ( $msg eq $breakon ) {
                $$err = [{$dir => "monkeypatch $msg"}];
                return;
            }
            make_path( $dir, { error => $err } );
        };

        failnice (sub{Yote::RecordStore->open_store($trydir)},
                  'monkeypatch base', 'no base dir' );
        $breakon = 'silo';
        failnice (sub{Yote::RecordStore->open_store($trydir)},
                  'monkeypatch silo', 'no silo dir' );
    }

    {
        my $breakon = 'index_silo';
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::_open_silo = sub {
            my ($self, $silo_file, $template, $size, $max_file_size ) = @_;
            if ($silo_file =~ qr/${breakon}$/) {
                die "monkeypatch $breakon";
            }
            return Yote::RecordStore::Silo->open_silo( $silo_file,
                                                             $template,
                                                             $size,
                                                             $max_file_size );
        };

        failnice (sub{Yote::RecordStore->open_store($dir)}, 'monkeypatch index_silo', 'no index silo' );

        $breakon = 'transaction_index_silo';
        failnice (sub {Yote::RecordStore->open_store($dir)}, 'monkeypatch transaction_index_silo', 'no transaction index silo' );

        $breakon = '12';
        failnice (sub {Yote::RecordStore->open_store($dir)}, 'monkeypatch 12', 'no data silo' );
    }


    {
        ok (-e "$dir/LOCK", "lock file exists");

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::_openhandle = sub {
            warn "monkeypatch _openhandle";
            return undef;
        };

        warnnice (sub{$rs->lock}, 1, 'monkeypatch _openhandle', "got lock with filehandle closed");
        is (flock( $rs->[Yote::RecordStore->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh filehandle closed' );
        ok ($rs->unlock, "unlock filehandle");
        unlink "$dir/LOCK";
        warnnice( sub{$rs->lock}, 1, 'monkeypatch _openhandle', "got lock with filehandle closed and lockfile removed");

        ok (-e "$dir/LOCK", "lock file regenerated");
        is (flock( $rs->[Yote::RecordStore->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh filehandle closed' );
        ok ($rs->unlock, "unlock filehandle");
        local *Yote::RecordStore::_open = sub {
            $@ = 'monkeypatch _open ';
            return undef;
        };
        failnice (sub{$rs->lock}, 'monkeypatch _open', "lock fail with open fail");
    }
    {
        ok (-e "$dir/LOCK", "lock file exists");
        ok ($rs->lock, 'lock works');
        ok ($rs->unlock, 'unlock works');

        ok ($rs->lock, 'lock works before _flock monkey');
        no strict 'refs';
        no warnings 'redefine';
        my $fail = 1;
        local *Yote::RecordStore::_flock = sub {
            my ($fh, $flags) = @_;
            if (++$fail) {
                $@ = "monkeypatch flock";
                return undef;
            }
            flock( $fh, $flags );
        };
        failnice (sub{$rs->unlock},
                  'unable to unlock',
                  'unlock fail due to monkeypatch');

        failnice (sub{$rs->lock},
                  'unable to lock',
                  'lock fail due to monkeypatch');

        $dir = tempdir( CLEANUP => 1 );
        failnice(sub{Yote::RecordStore->open_store($dir)},
                 'unable to lock',
                 'bad flock cant open' );
        $fail = -1;
        failnice(sub{Yote::RecordStore->open_store($dir)},
                 'unable to unlock',
                 'bad flock cant open' );
    }

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore->open_store("$dir/deeper");
    ok ($rs, 'make record store in non existing directory' );

    open my $in, '>', "$dir/deeper/VERSION";
    print $in "5.0";
    close $in;
    failnice( sub{Yote::RecordStore->open_store("$dir/deeper")},
              'Cannot open recordstore.*with version 5.0',
              'no opening previous version error msg');
    {
        # test lockfile cant be written
        my $old_open = *CORE::open;
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::_open = sub {
            my ($file) = @_;
            if ($file =~ /LOCK$/) {
                $@ = 'monkeypatch';
                return undef;
            }
            my $exists = -e $file;
            open my ($fh), $exists ? '+<' : '>', $file;
            unless ($exists) {
                print $fh '';
            }
            return $fh;
        };
        $dir = tempdir( CLEANUP => 1 );
        failnice( sub{Yote::RecordStore->open_store("$dir/deeper")},
                  'Error opening lockfile.*monkeypatch',
                  'make record store fail on lock' );
    }
    {
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore->open_store($dir);
        $rs->lock;

        is ($rs->stow( "BOOPO" ), 1, "first record again" );
        is ($rs->stow( "nuther", 1 ), 1, "first record again again" );

        is ($rs->stow( "too" ), 2, "second record again" );

        ok ($rs->delete_record(2), "second record deleted" );

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::Silo::put_record = sub {
            die 'monkeypatch put';
        };
        failnice (sub{$rs->stow("NYET")},
                  'monkeypatch put',
                  'No id for monkied put record on stow' );

        failnice (sub{$rs->delete_record(1)},
                  'monkeypatch put',
                  'No id for monkied put record on delete' );
    }
    is ($@, undef, 'BEEEE');

} #test_use

sub unlocks {
    my $store = shift;
    ok ($store->unlock,'unlock');
}
sub locks {
    my $store = shift;
    ok ($store->lock,'lock');
}

sub test_transactions {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore->open_store($dir);
    my $copy = Yote::RecordStore->open_store( $dir );

    locks ($copy);
    ok (!$@, 'no error after copy lock');
#    ok ($copy->is_locked, 'copy locked');
    my $id = $copy->stow( "SOMETHING ZOMETHING" );
    ok (!$@, 'no error after copy stow');
    is ( $id, 1, "first id" );
    my $oid = $copy->stow( "OMETHING ELSE" );
    ok (!$@, 'no error after copy stow 2');
    is ( $oid, 2, "second id" );
    ok( ! Yote::RecordStore->can_lock( $dir ), 'unable to lock directory while copy locks it' );

    unlocks ($copy);
    ok( Yote::RecordStore->can_lock( $dir ), 'now can lock directory since copy unlocked it' );

    locks ($rs);
    ok (!$@, 'no error after lock');
    failnice (sub{$rs->rollback_transaction},
              'no transaction to roll back',
              'cant roll back without transaction' );

    failnice (sub{$rs->commit_transaction},
              'no transaction to commit',
              "cant commit without transaction" );

    is ( $rs->transaction_silo->entry_count, 0, 'trans silo starts with no count' );

    unlocks ($rs);

    locks ($copy);
    is ( $copy->transaction_silo->entry_count, 0, 'copy trans silo starts with no count' );
    unlocks ($copy);

    locks ($rs);
    ok ($rs->use_transaction(), 'use transaction' );

    failnice (sub{$rs->delete_record( 12 )}, 'out of bounds', 'could not delete entry that did not exist' );
    eval {
        local( *STDERR );
        my $errout;
        open( STDERR, ">>", \$errout );
        $rs->use_transaction();
        like( $errout, qr/already in transaction/, 'already in transaction warning' );
        undef $@;
    };

    is ( $rs->transaction_silo->entry_count, 1, 'trans silo now with one count' );
    failnice (sub{$rs->unlock},
              'may not unlock with a pending',
              'cant unlock store with active transaction' );

    ok( ! Yote::RecordStore->can_lock( $dir ), 'rs is locked' );

    close $rs->[$rs->LOCK_FH];
    $rs->[$rs->IS_LOCKED] = 0;

    ok( Yote::RecordStore->can_lock( $dir ), 'rs not locked' );

    # see that there is one transaction that needs fixing
    my $trans_silo = $copy->transaction_silo;
    $trans_silo->reset;
    my $last_trans = $trans_silo->entry_count;
    is ($last_trans, 1, 'one transaction outstanding' );

    locks ($copy);
    is ( $copy->transaction_silo->entry_count, 0, 'copy trans silo had fixed transactions' );
    unlocks ($copy);

    # reet the trans silo to confirm it empty
    $trans_silo->reset;
    $last_trans = $trans_silo->entry_count;
    is ($last_trans, 0, 'no transactions outstanding' );


    locks ($rs);

    is ( $rs->transaction_silo->entry_count, 0, 'trans silo sees that copy trans silo had fixed transactions' );
    locks ($rs);
    ok ($rs->use_transaction, 'use trans');
    is ( $rs->fetch( $oid ), "OMETHING ELSE", 'trans has right val for something not in trans' );
    ok ($rs->stow( "THIS MEANS SOMETHING", $id ), 'stowed something in transaction');
    is ( $rs->fetch( $id ), "THIS MEANS SOMETHING", "correct value in transaction" );
    ok ($rs->commit_transaction, 'commit');
    unlocks ($rs);

    locks ($rs);
    is ( $rs->fetch( $id ), "THIS MEANS SOMETHING", "correct value after trans comitted" );
    unlocks ($rs);

    locks ($copy);

    is ( $copy->fetch( $id ), "THIS MEANS SOMETHING", "correct value from committed transaction $id" );

    unlocks ($copy);

    locks ($rs);
    my $newid = $rs->stow( "ONE MORE THING TO STOWWWWWWWW" );

#    is ( $newid, 3, '3rd object id' );
    is ( $rs->fetch( $newid ), "ONE MORE THING TO STOWWWWWWWW", "correct value assigned id" );
    unlocks ($rs);
    locks ( $copy );
    is ( $copy->fetch( $newid ), "ONE MORE THING TO STOWWWWWWWW", "correct value assigned id copy sees" );
    unlocks ($copy);

    locks ($rs);
    $rs->stow( "OH LETS CHANGE THIS UP", $newid );

    is ( $rs->fetch( $newid ), "OH LETS CHANGE THIS UP", "correct new value assigned id" );

    $rs->use_transaction;
    $rs->delete_record( $newid );
    is ($rs->fetch( $newid), undef, 'deleted in transaction' );
    $rs->rollback_transaction;
    is ($rs->fetch( $newid), "OH LETS CHANGE THIS UP", 'deletion rolled back' );

    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );

        locks $store;

        $store->stow( "A" );
        $store->stow( "B" );
        $store->stow( "C" );

        ok ($store->use_transaction);

        #    $store->delete_record( 1 );
        $store->stow( "D" );
        $store->stow( "E" );
        $store->stow( "F" );
        $store->delete_record( 2 );
        $store->delete_record( 3 );

        my $silo = $store->silos->[12];
        {
            # have vacate throw an exception, leaving things
            # in a state where records are marked as RS_DEAD
            # vacate is called in commit
            no strict 'refs';
            no warnings 'redefine';
            local *Yote::RecordStore::_vacate = sub {
                die "monkeywrench";
            };

            eval {
                $store->commit_transaction;
            };

            # verify silo is as expected
            is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,1,'A'], 'rec 1' );
            is_deeply( get_rec(2, $silo), [$rs->RS_DEAD,2,1,'B'], 'rec 2' );
            is_deeply( get_rec(3, $silo), [$rs->RS_DEAD,3,1,'C'], 'rec 3' );
            is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,1,'D'], 'rec 4' );
            is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,1,'E'], 'rec 5' );
            is_deeply( get_rec(6, $silo), [$rs->RS_ACTIVE,6,1,'F'], 'rec 6' );
            is (get_rec(7, $silo)->[0], $rs->RS_DEAD, 'transaction object dead' );

            is ($silo->entry_count, 7, '7 items' );
        }

        # now run the vacate manually. It should clear the last away and move record F
        is ($store->_vacate( 12, 2 ), 1, 'vacate worked');
        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,1,'A'], 'rec 1' );
        is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,6,1,'F'], 'rec 6' );
        is_deeply( get_rec(3, $silo), [$rs->RS_DEAD,3,1,'C'], 'rec 3' );
        is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,1,'D'], 'rec 4' );
        is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,1,'E'], 'rec 5' );

        is ($silo->entry_count, 5, '5 items' );
    }

    # test some transaction fails
    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );

        locks ($store);

        my $tsilo = $store->transaction_silo;
        is ($tsilo->entry_count, 0, 'no entries in trans silo');

        my $silo = $store->silos->[12];
        is ($silo->entry_count, 0, 'no entries in silo');

        is ($store->stow( "A" ), 1, "first id");
        is ($store->stow( "B" ), 2, 'id 2' );
        is ($silo->entry_count, 2, '2 entries in silo');

        $store->use_transaction;
        is ($tsilo->entry_count, 1, 'one entries in trans silo');

        is ($store->stow( "RA" ), 3, 'id 3' );
        is ($store->stow( "RB" ), 4, 'id 4' );
        is ($silo->entry_count, 4, '4 entries in silo');
        is ($store->fetch(3), 'RA', 'value before commit' );
        is ($store->fetch(4), 'RB', 'value before commit' );

        # destroy transaction and unlock for test
        $store->[$store->TRANSACTION] = undef;
        unlocks ($store);

        #
        # pretend store is locked to be able to fetch things
        #
        $store->[$store->IS_LOCKED] = 1;
        is ($silo->entry_count, 4, 'still 4 entries in silo');

        is ($store->fetch(1), 'A', 'first fe' );
        is ($store->fetch(2), 'B', 'sec fe' );
        is ($store->fetch(3), undef, 'not commited dead val 1' );
        is ($store->fetch(4), undef, 'not commited dead val 2' );

        #
        # Try to reset the store now which will fix the transaction
        #
        $store->_reset;
        is ($tsilo->entry_count, 0, 'fixed cleared out trans silo');
        is ($silo->entry_count, 4, 'still 4 entries since those 2 were not noted or cleaned up');
    }

    {
        $dir = tempdir( CLEANUP => 1 );

        my $rs = Yote::RecordStore->open_store( $dir );

        my $silo = $rs->silos->[12];
        is ($silo->entry_count, 0, 'silo starts off empty' );

        locks ($rs);

        my $id = $rs->stow( "ZERO" );

        is ($silo->entry_count, 1, 'silo now has one' );

        ok ($rs->use_transaction);

        $rs->stow( "ONE" );
        $rs->stow( "TWO" );
        $rs->stow( "THREE" );

        my $trans = $rs->[$rs->TRANSACTION];
        $rs->[$rs->TRANSACTION] = undef;

        $id = $rs->stow( "GOING" );

        is ($silo->entry_count, 5, 'silo with 5 items' );

        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,4,'ZERO'], 'rec 1' );
        is_deeply( get_rec(2, $silo), [$rs->RS_IN_TRANSACTION,2,3,'ONE'], 'rec 2' );
        is_deeply( get_rec(3, $silo), [$rs->RS_IN_TRANSACTION,3,3,'TWO'], 'rec 3' );
        is_deeply( get_rec(4, $silo), [$rs->RS_IN_TRANSACTION,4,5,'THREE'], 'rec 4' );
        is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,5,'GOING'], 'rec 5' );

        # simulate transaction saved but not carried out
        ok ($trans->_save, 'trans could save');
        is ($silo->entry_count, 6, 'silo with 6 items after trans save' );

        # now try to clean up things

        $rs->_fix_transactions;

    }

    # test commit transaction fail
    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );

        locks ($store);

        my $tsilo = $store->transaction_silo;
        is ($tsilo->entry_count, 0, 'no entries in trans silo');

        my $silo = $store->silos->[12];
        is ($silo->entry_count, 0, 'no entries in silo');

        is ($store->stow( "A" ), 1, "first id");
        is ($store->stow( "B" ), 2, 'id 2' );
        is ($silo->entry_count, 2, '2 entries in silo');

        $store->use_transaction;
        is ($tsilo->entry_count, 1, 'one entries in trans silo');

        is ($store->stow( "RA" ), 3, 'id 3' );
        is ($store->stow( "RB" ), 4, 'id 4' );

        # cause commit to fail. muhahah
        no warnings 'redefine';
        no strict 'refs';

        my $t = 0;

        {
            local *Yote::RecordStore::Silo::put_record = sub {
                if ($t++ > 1) {
                    die 'monkey';
                }
                my $self = shift;
                $self->_put_record( @_ );
            };

            failnice (sub {$store->commit_transaction}, 'monkey', 'broken commit' );

            failnice (sub {$store->rollback_transaction}, 'monkey', 'broken rollback' );
        }
    }

    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );
        my $silo = $store->silos->[12];
        my $isilo = $store->index_silo;

        locks ($store);
        is ($store->stow( "WOOF" ), 1, 'first woof' );
        is ($silo->entry_count, 1, 'one entries in transhy');
        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,4,'WOOF'], 'rec in store' );
        $store->delete_record( 1 );

        is ($isilo->entry_count, 1, 'one index entry');

        my $trans = $store->use_transaction;

        ok( $trans );

        failnice (sub{$store->delete_record( 100 )}, 'out of bounds', 'could not delete entry that never existed' );

        is ($store->delete_record( 1 ), undef, 'could not delete entry that did not exist' );
        is ($store->stow( "BBB" ), 2, 'second id, but only for trans' );

        is_deeply( get_rec(1, $silo), [$rs->RS_DEAD,1,4,'WOOF'], 'deleted rec 1 before commit' );

        is ($isilo->entry_count, 2, 'two index entry');

        ok ($store->commit_transaction);

        is_deeply( get_rec(1, $silo), [$rs->RS_DEAD,1,4,'WOOF'], 'rec 2 after commit is first in silo' );

        is ($isilo->entry_count, 3, 'three index entry after commit');
print STDERR Data::Dumper->Dump(["BREAKS HERe because it has been committed, so the transaction should have its obj id removed"]);
        
        $trans->fix;
print STDERR Data::Dumper->Dump(["A"]);
    }

    # put_record doesnt work for
    #    delete case
    #    add case
    # use for record
    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );
        my $silo = $store->silos->[12];
        my $tsilo = $store->transaction_silo;
        my $isilo = $store->index_silo;
print STDERR Data::Dumper->Dump(["A"]);
        locks ($store);
print STDERR Data::Dumper->Dump(["B"]);
        my $failid = [];
        {
            no warnings 'redefine';
            no strict 'refs';
            local *Yote::RecordStore::Silo::put_record = sub {
                my $self = shift;
                my( $id, $data, $template, $offset ) = @_;
                if (compare_arrays( $data, $failid)) {
                    die 'monkey';
                }
                $self->_put_record( @_ );
            };
            locks ( $store );

            is ($isilo->entry_count, 0, 'no rec in index silo' );

            $store->use_transaction;

            $failid = [12,1];

            $store->stow( "AOOU" );

            is ($isilo->entry_count, 1, '1 rec in index silo' );

            is_deeply( get_rec( 1, $isilo, 2), [ 0, 0 ], 'index rec is empty because no commit yet' );
            is ($silo->entry_count, 1, 'the stowed transaction data' );

            is_deeply( get_rec( 1, $silo), [ $store->RS_IN_TRANSACTION, 1, 4, 'AOOU' ], 'stowed transaction data' );

            failnice (sub{$store->commit_transaction}, 'monkey', 'could not commit due to monkey' );

            is ($isilo->entry_count, 2, '2 rec in index silo now, dead record and transaction' );

            is_deeply( get_rec( 1, $isilo, 2), [ 0, 0 ], 'index rec is unchanged' );
            is_deeply( get_rec( 2, $isilo, 2), [ 12, 2 ], 'index rec is unchanged' );

            is ($tsilo->entry_count, 1, 'one transaction');
            is_deeply( get_rec( 1, $tsilo), [ $store->TR_IN_COMMIT, 2 ], 'state of transaction silo' );

            $store->_fix_transactions;
            is ($isilo->entry_count, 2, 'has stow and transaction object' );
            is ($tsilo->entry_count, 0, 'no transactions');
            is ($silo->entry_count, 2, 'still 2 entries');

            is_deeply( get_rec( 1, $silo), [ $store->RS_DEAD, 1, 4, 'AOOU' ], 'index rec is unchanged' );
            my $x = get_rec( 2, $silo);
            is_deeply( get_rec( 2, $silo, 3), [ $store->RS_DEAD, 2, 24 ], 'transaction object not yet cleared out' );
            is_deeply( [unpack( "IIIIII", $x->[3])], [$store->RS_ACTIVE,1,0,0,12,1], 'transaction object values' );
            is_deeply( get_rec( 2, $silo), [ $store->RS_DEAD, 2, 24, pack ("IIIIII", $store->RS_ACTIVE,1,0,0,12,1 ) ], 'transaction object not yet cleared out' );

            $store->_vacuum;
            is ($silo->entry_count, 0, '2 entries cleaned up');
        }

    }

    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );
        locks ($store);
        my $silo = $store->silos->[12];
        my $bigsilo = $store->silos->[13];

        $store->stow( "OOGA" );
        $store->use_transaction;
        is ($silo->entry_count, 1, 'one thing' );
        is ($store->stow( "TOOA" ), 2, 'second thing' );
        is_deeply( get_rec( 2, $silo), [ $store->RS_IN_TRANSACTION, 2, 4, 'TOOA' ], 'transaction added entry' );
        $store->delete_record( 2 );
        $store->delete_record( 2 );

        $store->delete_record( 1 );
        $store->delete_record( 1 );

        $silo->reset;
        is_deeply( get_rec( 2, $silo), [ $store->RS_DEAD, 2, 4, 'TOOA' ], 'transaction added then removed entry' );

        $store->commit_transaction;
        $silo->reset;
        is_deeply( get_rec( 2, $silo), [ $store->RS_DEAD, 2, 4, 'TOOA' ], 'transaction added then removed entry after commit' );
        is_deeply( get_rec( 1, $silo), [ $store->RS_DEAD, 1, 4, 'OOGA' ], 'transaction added then removed entry after commit' );
    }

    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore->open_store( $dir );
        locks ($store);
        my $silo = $store->silos->[12];
        my $bigsilo = $store->silos->[13];

        $store->stow( "OOGA" );
        my $large = "X" x 7_000;
        $store->stow( $large );
        is ($bigsilo->entry_count, 1, '1 big thing' );
        
    }

} #test_transactions

sub compare_arrays {
    my ($a1, $a2) = @_;
    return if @$a1 != @$a2;
    for (my $i=0; $i<@$a1; $i++ ) {
        return if $a1->[$i] ne $a2->[$i]
    }
    return 1;
}

sub test_sillystrings {

    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore->open_store( $dir );
    locks $store;
    my $packed = pack( "I*", (0..100) );
    is ( $store->stow( $packed ), 1, "id 1 for stowing silly" );
    is ( $store->fetch(1), $packed, "packed string worked" );

} #test_sillystrings


sub test_meta {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore->open_store( $dir );

    locks $store;

    warnnice (sub{$store->fetch_meta( 3 )},
              undef,
              'past end of records',
              'no meta to fetch' );
    {
        no warnings 'redefine';
        no strict 'refs';

        my $t = 0;

        local *Yote::RecordStore::_time = sub {
            $t;
        };

        $t = 1000;
        is ($store->stow( "THISISATEST" ), 1, 'stowed a record' );

        my ($upd, $cr ) = $store->fetch_meta( 1 );
        is ($upd, 1000, 'has a last updated time' );
        is ($cr, 1000, 'has a created time' );

        $t = 2000;

        $store->stow( "BLLBLBBL", 1 );

        my ($upd2, $cr2 ) = $store->fetch_meta( 1 );

        is ($upd2, 2000, 'updated time is greater' );
        is ($cr2, $cr, 'created time did not change' );
    }
}

package Factory;

use File::Temp qw/ :mktemp tempdir /;

sub new { return bless {}, shift }

sub new_rs {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore->open_store($dir);

    return $store;
}
sub reopen {
    my( $cls, $oldstore ) = @_;
    return Yote::RecordStore->open_store( $oldstore->[Yote::RecordStore->DIRECTORY] );
}

__END__
