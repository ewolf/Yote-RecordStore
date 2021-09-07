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
use Fcntl qw(:mode :flock SEEK_SET);
use File::Temp qw/ :mktemp tempdir /;
use File::Path qw/ remove_tree /;
use Scalar::Util qw(openhandle);
use Test::More;
#use Errno qw(ENOENT);


use Carp;
$SIG{ __DIE__ } = sub { Carp::confess( @_ ) }; 

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

#api->test_failed_async( $factory );
#api->test_transaction_async( $factory );
#api->test_locks_async( $factory );
#api->test_suite_recordstore( $factory );


done_testing;
exit;

sub failnice {
    my( $res, $errm, $msg ) = @_;
    is ($res, undef, $msg );
    like( $@, qr/$errm/, "$msg error" );
    undef $@;
}

sub get_rec {
    my( $id, $silo) = @_;
    my $res = $silo->get_record($id);
    $res->[3] = substr( $res->[3], 0, $res->[2] );
    $res;
}


sub test_vacate {
    my $dir = tempdir( CLEANUP => 1 );

    my $rs = Yote::RecordStore::File->open_store( $dir );

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

    unlocks ($rs);

    locks ($rs);

    my $id = $rs->stow( "GOING" );
    unlocks ($rs);

    is ($silo->entry_count, 5, 'silo with 5 items' );
    
    is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,4,'ZERO'], 'rec 1' );
    is_deeply( get_rec(2, $silo), [$rs->RS_IN_TRANSACTION,2,3,'ONE'], 'rec 2' );
    is_deeply( get_rec(3, $silo), [$rs->RS_IN_TRANSACTION,3,3,'TWO'], 'rec 3' );
    is_deeply( get_rec(4, $silo), [$rs->RS_IN_TRANSACTION,4,5,'THREE'], 'rec 4' );
    is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,5,'GOING'], 'rec 5' );

    # silo with [ ZERO, xx, xx, xx, GOING ]
    # with vacate should go to
    # silo with [ ZERO, GOING ]
    #$rs->_vacate( 12, 2 );
    $trans->rollback;

    $silo->reset;
    is ($silo->entry_count, 2, 'silo now with 2 items' );

    {
        # test an interrupted stow with items marked RS_IN_TRANSACTION
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore::File->open_store( $dir );

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
        
        failnice ($store->_vacate( 12, 2 ), 
                  "got record state ".$rs->RS_IN_TRANSACTION.". not popping",
                  'vacate failed because of entries in transaction');
        is ($silo->entry_count, 4, '4 silo entries' );
    }

    {
        # test an interrupted stow with items marked RS_IN_TRANSACTION
        $dir = tempdir( CLEANUP => 1 );

        my $store = Yote::RecordStore::File->open_store( $dir );

        locks ($store);
        
        $store->stow( "A" );

        $store->use_transaction;

        $store->stow( "TB" );
        $store->stow( "TC" );
        $store->stow( "TD" );

        $store->commit_transaction;

        my $silo = $store->silos->[12];

        # now it should be at a state like so:
        is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,1,1,'A'], 'rec 1' );
        is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,2,2,'TB'], 'rec 2' );
        is_deeply( get_rec(3, $silo), [$rs->RS_ACTIVE,3,2,'TC'], 'rec 3' );
        is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,2,'TD'], 'rec 4' );
        is ($silo->entry_count, 4, '4 silo entries' );

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::Silo::copy_record = sub {
            $@ = 'monkey wrench';
            return undef;
        };
        
        failnice ($store->_vacate( 12, 2 ), 
                  'unable to copy record',
                  'vacate failed because of entries in transaction');
        is ($silo->entry_count, 4, 'still 4 silo entries' );
    }

    
}

sub test_init {
    my $dir = tempdir( CLEANUP => 1 );

    is (Yote::RecordStore::File->first_id, 1, 'first id');

    is (Yote::RecordStore::File->detect_version( $dir ), undef, 'no version file yet');

    my $rs = Yote::RecordStore::File->open_store( $dir );

    ok (Yote::RecordStore::File->detect_version( $dir ) > 0, 'version file has version');
    is (Yote::RecordStore::File->detect_version( $dir ), Yote::RecordStore::File->VERSION, 'version file with correct version');

    ok( $rs, 'inited store' );
    is ($rs->directory, $dir, "recordstore directory" );

    is ( $rs->[Yote::RecordStore::File->MIN_SILO_ID], 12, "default min silo id" );
    is ( $rs->[Yote::RecordStore::File->MAX_SILO_ID], 31, "default max silo id" );

    my $silos = $rs->silos;
    is ( @$silos - $rs->[Yote::RecordStore::File->MIN_SILO_ID], 1 + (31 - 12), '20 silos' );
    $rs->[$rs->LOCK_FH] = undef;

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
    #     failnice( Yote::RecordStore::File->open_store("$dir/cant"),
    #               'permission denied',
    #               'made a directory that it could not' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     my $lockfile = "$dir/LOCK";
    #     open my $out, '>', $lockfile;
    #     print $out '';
    #     close $out;
    #     chmod 0444, $lockfile;
    #     failnice(
    #         Yote::RecordStore::File->open_store( $dir ),
    #               "permission denied",
    #               "was able to init store with unwritable lock file" );

    #     $dir = tempdir( CLEANUP => 1 );
    #     Yote::RecordStore::File->open_store( $dir );
    #     chmod 0000, "$dir/LOCK";
    #     failnice( Yote::RecordStore::File->open_store( $dir ),
    #               'permission denied',
    #               'was able to reopen store with unwritable lock file' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     chmod 0444, "$dir";
    #     failnice( Yote::RecordStore::File->open_store( $dir ),
    #               'permission denied',
    #               'was not able to open store in unwritable directory' );

    #     $dir = tempdir( CLEANUP => 1 );
    #     open $out, ">", "$dir/VERSION";
    #     print $out "666\n";
    #     close $out;
    #     failnice( Yote::RecordStore::File->open_store( $dir ),
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
            $store = Yote::RecordStore::File->open_store( $dir );
            pass( "Was able to open store" );
            chmod 0444, "$dir/user_locks";

            $store->lock( "FOO" );
            fail( "Yote::RecordStore::File->lock didnt die trying to lock to unwriteable directory" );
        };
        like( $@, qr/lock failed/, "unable to lock because of unwriteable lock directory" );
        undef $@;

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
        undef $@;
    }
} #test_locks

sub test_use {
    my $dir = tempdir( CLEANUP => 1 );
    my $rs = Yote::RecordStore::File->open_store($dir);

    failnice ($rs->record_count ,
               'store not locked',
               'cant use record_count until store is locked' );

    failnice ($rs->active_entry_count ,
               'store not locked',
               'cant use active_entry_count until store is locked' );

    failnice ($rs->stow( "THE FIRST" ) ,
               'store not locked',
               'cant use stow until store is locked' );

    failnice ($rs->fetch(1) ,
               'store not locked',
               'cant use fetch until store is locked' );

    failnice ($rs->fetch_meta(1) ,
               'store not locked',
               'cant use fetch_meta until store is locked' );

    failnice ($rs->unlock(1) ,
               'store not locked',
               'cant use unlock until store is locked' );

    failnice ($rs->next_id ,
               'store not locked',
               'cant use next_id until store is locked' );

    failnice ($rs->delete_record(12) ,
               'store not locked',
               'cant use delete_record until store is locked' );

    failnice ($rs->silos_entry_count ,
               'store not locked',
               'cant use silos_entry_count until store is locked' );


    failnice ($rs->use_transaction ,
               'store not locked',
               'cant use use_transaction until store is locked' );

    failnice ($rs->commit_transaction ,
               'store not locked',
               'cant use commit_transaction until store is locked' );

    failnice ($rs->rollback_transaction ,
               'store not locked',
               'cant use rollback_transaction until store is locked' );


    ok ( ! $rs->is_locked, 'not locked' );

    is ($@, undef, 'no error after lock check');


    ok ($rs->lock, 'able to lock record store');
    is ( $rs->is_locked, 1, 'now locked' );

    failnice ($rs->rollback_transaction ,
               'no transaction',
               'cant use rollback_transaction until store is locked' );


    failnice ($rs->delete_record(12) ,
               'past end of',
               'cant use delete_record until store is locked' );

    failnice ($rs->stow("GAWOOOUUNGA", 0) ,
               'must be a positive integer',
               'cant use delete_record until store is locked' );

    failnice ($rs->stow("GAWOOOUUNGA", 1.5) ,
               'must be a positive integer',
               'cant use delete_record until store is locked' );

    failnice ($rs->stow("GAWOOOUUNGA", -2) ,
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
    failnice ($rs->fetch( 22 ),
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
    $rs = Yote::RecordStore::File->open_store($dir);
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
    is_deeply( get_rec(1, $silo), [$rs->RS_ACTIVE,6,4,'SOOZ'], 'rec 1' );
    is_deeply( get_rec(2, $silo), [$rs->RS_ACTIVE,2,5,'BLINK'], 'rec 2' );
    is_deeply( get_rec(3, $silo), [$rs->RS_ACTIVE,3,3,'ZAP'], 'rec 3' );
    is_deeply( get_rec(4, $silo), [$rs->RS_ACTIVE,4,5,'XZOOR'], 'rec 4' );
    is_deeply( get_rec(5, $silo), [$rs->RS_ACTIVE,5,6,'ZSMOON'], 'rec 5' );

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

    ok ($rs->lock, "got lock");
    is (flock( $rs->[Yote::RecordStore::File->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh' );
    ok ($rs->unlock, "unlock");

    {
        my $trydir = tempdir( CLEANUP => 1 ) . '/addy';
        my $breakon = 'base';
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::_make_path = sub {
            my ( $dir, $err, $msg ) = @_;
            if ( $msg eq $breakon ) {
                $$err = [{$dir => "monkeypatch $msg"}];
                return;
            }
            make_path( $dir, { error => \$err } );
        };

        failnice (Yote::RecordStore::File->open_store($trydir),
                  'monkeypatch base', 'no base dir' );
        $breakon = 'silo';
        failnice (Yote::RecordStore::File->open_store($trydir),
                  'monkeypatch silo', 'no silo dir' );
    }

    {
        my $breakon = 'index_silo';
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::_open_silo = sub {
            my ($self, $silo_file, $template, $size, $max_file_size ) = @_;
            if ($silo_file =~ qr/${breakon}$/) {
                $@ = "monkeypatch $breakon";
                return undef;
            }
            return Yote::RecordStore::File::Silo->open_silo( $silo_file,
                                                             $template,
                                                             $size,
                                                             $max_file_size );
        };

        is (Yote::RecordStore::File->open_store($dir), undef, 'no index silo' );
        like ($@, qr/monkeypatch index_silo/, 'no index silo message');
        undef $@;

        $breakon = 'transaction_index_silo';
        is (Yote::RecordStore::File->open_store($dir), undef, 'no transaction index silo' );
        like ($@, qr/monkeypatch transaction_index_silo/, 'no transaction silo message');
        undef $@;

        $breakon = '12';
        is (Yote::RecordStore::File->open_store($dir), undef, 'no data silo' );
        like ($@, qr/monkeypatch 12/, 'no data silo message');
        undef $@;
    }
    

    {
        ok (-e "$dir/LOCK", "lock file exists");

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::_openhandle = sub {
            $@ = "monkeypatch _openhandle";
            return undef;
        };
        
        ok ($rs->lock, "got lock with filehandle closed");
        like ($@, qr/monkeypatch _openhandle/, 'openhandle failed message');
        undef $@;
        is (flock( $rs->[Yote::RecordStore::File->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh filehandle closed' );
        ok ($rs->unlock, "unlock filehandle");

        unlink "$dir/LOCK";
        ok ($rs->lock, "got lock with filehandle closed and lockfile removed");
        like ($@, qr/monkeypatch _openhandle/, 'openhandle failed message');
        undef $@;

        ok (-e "$dir/LOCK", "lock file regenerated");
        is (flock( $rs->[Yote::RecordStore::File->LOCK_FH], LOCK_NB || LOCK_EX ), 0, 'unable to lock already locked fh filehandle closed' );
        ok ($rs->unlock, "unlock filehandle");
        
        local *Yote::RecordStore::File::_open = sub {
            $@ = 'monkeypatch _open ';
            return undef;
        };
        failnice ($rs->lock, 'monkeypatch _open', "lock fail with open fail");
    }
    {
        ok (-e "$dir/LOCK", "lock file exists");
        ok ($rs->lock, 'lock works');
        ok ($rs->unlock, 'unlock works');

        ok ($rs->lock, 'lock works before _flock monkey');
        no strict 'refs';
        no warnings 'redefine';
        my $fail = 1;
        local *Yote::RecordStore::File::_flock = sub {
            my ($fh, $flags) = @_;
            if ($fail) {
                $@ = "monkeypatch flock";
                return undef;
            }
            flock( $fh, $flags );
        };
        failnice ($rs->unlock, 
                  'monkeypatch flock',
                  'unlock fail due to monkeypatch');

        failnice ($rs->lock, 
                  'monkeypatch flock',
                  'lock fail due to monkeypatch');
    }

    $dir = tempdir( CLEANUP => 1 );
    $rs = Yote::RecordStore::File->open_store("$dir/deeper");
    ok ($rs, 'make record store in non existing directory' );

    open my $in, '>', "$dir/deeper/VERSION";
    print $in "5.0";
    close $in;
    failnice( Yote::RecordStore::File->open_store("$dir/deeper"),
              'Cannot open recordstore.*with version 5.0',
              'no opening previous version error msg');
    {
        # test lockfile cant be written
        my $old_open = *CORE::open;
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::_open = sub {
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
        failnice( Yote::RecordStore::File->open_store("$dir/deeper"),
                  'Error opening lockfile.*monkeypatch',
                  'make record store fail on lock' );
    }
    {
        $dir = tempdir( CLEANUP => 1 );
        $rs = Yote::RecordStore::File->open_store($dir);
        $rs->lock;

        is ($rs->stow( "BOOPO" ), 1, "first record again" );
        is ($rs->stow( "nuther", 1 ), 1, "first record again again" );

        is ($rs->stow( "too" ), 2, "second record again" );

        ok ($rs->delete_record(2), "second record deleted" );

        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::Silo::put_record = sub {
            $@ = 'monkeypatch put';
            return undef;
        };
        failnice ($rs->stow("NYET"),
                  'monkeypatch put',
                  'No id for monkied put record on stow' );

        failnice ($rs->delete_record(1),
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
    my $rs = Yote::RecordStore::File->open_store($dir);
    my $copy = Yote::RecordStore::File->open_store( $dir );

    locks ($copy);
    ok (!$@, 'no error after copy lock');
#    ok ($copy->is_locked, 'copy locked');
    my $id = $copy->stow( "SOMETHING ZOMETHING" );
    ok (!$@, 'no error after copy stow');
    is ( $id, 1, "first id" );
    my $oid = $copy->stow( "OMETHING ELSE" );
    ok (!$@, 'no error after copy stow 2');
    is ( $oid, 2, "second id" );
    ok( ! Yote::RecordStore::File->can_lock( $dir ), 'unable to lock directory while copy locks it' );

    unlocks ($copy);
    ok( Yote::RecordStore::File->can_lock( $dir ), 'now can lock directory since copy unlocked it' );

    locks ($rs);
    ok (!$@, 'no error after lock');
    failnice ($rs->rollback_transaction,
              'no transaction to roll back', 
              'cant roll back without transaction' );

    failnice ($rs->commit_transaction, 
              'no transaction to commit', 
              "cant commit without transaction" );

    is ( $rs->transaction_silo->entry_count, 0, 'trans silo starts with no count' );

    unlocks ($rs);

    locks ($copy);
    is ( $copy->transaction_silo->entry_count, 0, 'copy trans silo starts with no count' );
    unlocks ($copy);
    
    locks ($rs);
    ok ($rs->use_transaction(), 'use transaction' );

    eval {
        local( *STDERR );
        my $errout;
        open( STDERR, ">>", \$errout );
        $rs->use_transaction();
        like( $errout, qr/already in transaction/, 'already in transaction warning' );
        undef $@;
    };

    is ( $rs->transaction_silo->entry_count, 1, 'trans silo now with one count' );
    failnice ($rs->unlock,
              'may not unlock with a pending',
              'cant unlock store with active transaction' );

    ok( ! Yote::RecordStore::File->can_lock( $dir ), 'rs is locked' );

    close $rs->[$rs->LOCK_FH];
    $rs->[$rs->IS_LOCKED] = 0;

    ok( Yote::RecordStore::File->can_lock( $dir ), 'rs not locked' );

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

# return;
#     my $nid = $rs->stow( "FORGRABS" );
#     $copy->_reset;
#     my $nextid = $copy->next_id;
#     is ( $nextid, 1 + $nid, 'copy gets one id further than trans store' );
#     is ( $rs->fetch( $nid ), "FORGRABS", "correct new value assigned id" );
#     is ( $copy->fetch( $nid ), undef, "copy still cant see commit value" );

#     $rs->delete_record( $nid );
#     is ( $rs->fetch( $nid ), undef, "correct deleted assigned id" );
#     is ( $copy->fetch( $nid ), undef, "copy still cant see commit value" );

#     $copy->stow( "neet", $nextid );
#     $rs->_reset;
#     is ( $rs->fetch( $nextid ), 'neet', "trans sees copy stow" );
#     is ( $copy->fetch( $nextid ), 'neet', "copy sees own stow" );

#     $rs->delete_record( $nextid );
#     is ( $rs->fetch( $nextid ), undef, "trans cant see deleted item" );
#     is ( $copy->fetch( $nextid ), 'neet', "copy still sees own stow" );

#     $rs->stow( "X" x 10_000 );

#     ok ($rs->commit_transaction(), 'committed the transaction' );
#     is ($rs->[$rs->TRANSACTION], undef, 'no more transaction' );

#     $copy->_reset;
#     is ( $copy->fetch( $id ), "THIS MEANS SOMETHING", "correct value in copy after commit" );
#     is ( $rs->fetch( $id ), "THIS MEANS SOMETHING", "correct value in after commit" );

#     is ( $rs->fetch( $nextid ), undef, "trans cant see deleted item after commit" );
#     is ( $copy->fetch( $nextid ), undef, "copy sees deleted own stow after commit" );

#     is ( $copy->fetch( $nextid + 1 ), "X" x 10_000, 'big thing saved' );

#     $dir = tempdir( CLEANUP => 1 );
#     $rs = Yote::RecordStore::File->open_store($dir);
#     $copy = Yote::RecordStore::File->open_store( $dir );
    
#     $rs->use_transaction;
    
#     $newid = $rs->stow( "WIBBLES AND FOOKZA" );
#     $id = $copy->stow( "SOMETHING TO NEARLY DELETE" );
#     $rs->_reset;
#     $rs->delete_record( $id );

#     is ( $rs->fetch( $id ), undef, "trans cant see deleted item before commit" );
#     is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );

#     $rs->stow( "BLAME ME", $id );

#     is ( $rs->fetch( $id ), "BLAME ME", "trans sees updated before commit" );
#     is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );

#     $rs->delete_record( $id );

#     $rs->delete_record( $id );

#     is ( $rs->fetch( $id ), undef, "trans cant see deleted item before commit" );
#     is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );
    
#     is ( $rs->fetch( $newid ), "WIBBLES AND FOOKZA", "trans can see stowed item before rollback" );
#     is ( $copy->fetch( $newid ), undef, "copy cant see stowed item before rollback" );

#     $rs->rollback_transaction;

#     is ( $rs->fetch( $id ), "SOMETHING TO NEARLY DELETE", "rs sees thing after transaction delete rollback" );
#     is ( $copy->fetch( $id ), "SOMETHING TO NEARLY DELETE", "copy sees thing after transaction delete no commit" );
#     is ( $rs->fetch( $newid ), undef, "trans cant see stowed item after rollback" );
#     is ( $copy->fetch( $newid ), undef, "copy cant see stowed item after rollback" );

    
    # need to monkey patch to test rollback of partially commited thing
    
    # $dir = tempdir( CLEANUP => 1 );
    # $rs = Yote::RecordStore::File->open_store($dir);
    # $copy = Yote::RecordStore::File->open_store( $dir );

    # my $times = 100_000;

    # locks ($copy);
    # my $id1 = $copy->stow( "THIS IS THE FIRST ONE IN THE FIRST PLACE" );
    # my $id2 = $copy->stow( "DELETE ME OR TRY TO" );
    # my $id3 = $copy->stow( "OH, CHANGE THIS REALLY BIG THING"x$times );
    # my $id4 = $copy->stow( "CHANGE ME UP" );
    # unlocks ($copy);

    # locks ($rs);
    # my( $breakid );

    # $rs->use_transaction;
    
    # $rs->stow( "GROWING IT UP"x$times, $id1 );
    # $rs->delete_record( $id2 );

    # $rs->stow( "SHRINKING THIS DOWN", $id3 );
    # $rs->stow( "CHANGED THIS UP", $id4 );
    
    # $breakid = $rs->stow( "BREAK ON THIS" );

    # no warnings 'redefine';
    # no strict 'refs';
    # local *Yote::RecordStore::File::Silo::put_record = sub {
    #     my( $self, $id, $data, $template, $offset ) = @_;
    #     if( $self->[Yote::RecordStore::File::Silo->CUR_COUNT+1] ) {
    #         if( $id == $breakid ) {
    #             $@ = "Breakpoint";
    #             return undef;
    #         }
    #     }

    #     if( $id > $self->entry_count || $id < 1 ) {
    #         $@ = "Yote::RecordStore::File::Silo->put_record : index $id out of bounds for silo $self->[0]. Store has entry count of ".$self->entry_count;
    #         return undef;
    #     }
    #     if( ! $template ) {
    #         $template = $self->[Yote::RecordStore::File::Silo->TEMPLATE];
    #     }

    #     my $rec_size = $self->[Yote::RecordStore::File::Silo->RECORD_SIZE];
    #     my $to_write = pack ( $template, ref $data ? @$data : $data );
    #     # allows the put_record to grow the data store by no more than one entry
    #     my $write_size = do { use bytes; length( $to_write ) };
    #     if( $write_size > $rec_size) {
    #         $@ = "Yote::RecordStore::File::Silo->put_record : record size $write_size too large. Max is $rec_size";
    #         return undef;
    #     }

    #     my( $idx_in_f, $fh, $subsilo_idx ) = $self->_fh( $id );

    #     $offset //= 0;
    #     my $seek_pos = $rec_size * $idx_in_f + $offset;
    #     sysseek( $fh, $seek_pos, SEEK_SET );
    #     syswrite( $fh, $to_write );

    #     return 1;
    # };
    # use strict 'refs';


    # $rs->index_silo->[Yote::RecordStore::File::Silo->CUR_COUNT+1] = 1;

    # is ( $rs->fetch( $id1 ), "GROWING IT UP"x$times, "in trans with correct value" );
    # is ( $rs->fetch( $id2 ), undef, "deleted in trans" );
    # is ( $rs->fetch( $id3 ), "SHRINKING THIS DOWN", "other changed val in trans");
    # is ( $rs->fetch( $id4 ), "CHANGED THIS UP", "~ same size thing changed in trans");
    # is ($rs->commit_transaction, undef,  "cant commit due to monkeypatched error" );
    # like ($@, qr/Breakpoint/, 'break msg' );
    # undef $@;

    # # okey, that transaction kinda went away
    # # so load it up, I guess
    # is ($rs->[$rs->TRANSACTION_INDEX_SILO]->entry_count, 1, 'one transaction' );

    # my $trans = $rs->[$rs->TRANSACTION];
    # is ( $trans->{state}, Yote::RecordStore::File::Transaction::TR_IN_COMMIT, "attached transaction is in commit" );

    # $trans = Yote::RecordStore::File::Transaction->open( $rs, 1 );

    # is ( $trans->{state}, Yote::RecordStore::File::Transaction::TR_IN_COMMIT, "transaction is in commit" );

    # is ( $rs->fetch( $id1 ), undef, 'fetch returns undef when in bad state' );
    # like ($@, qr/Transaction is in a bad state/ );
    # undef $@;

    # ok ($rs->is_locked, 'rs is locked');

    # unlocks ($rs);

    # ok ( ! $rs->is_locked, 'rs no longer locked');

    # # now we gotta think about what might happen to the copy. maybe talk about locks?
    # locks ($copy);

    # # test if the state of the recordstore fixes itself with transaction in bad state
    # is ( $copy->fetch( $id1 ), "THIS IS THE FIRST ONE IN THE FIRST PLACE", "id 1 unchanged" );
    # is ( $copy->fetch( $id2 ), "DELETE ME OR TRY TO", "id 2 unchanged" );
    # is ( $copy->fetch( $id3 ), "OH, CHANGE THIS REALLY BIG THING"x$times, "id 3 unchanged" );
    # is ( $copy->fetch( $id4 ), "CHANGE ME UP", "id 4 unchanged" );
        
    # use strict 'refs';

    # $rs->index_silo->[Yote::RecordStore::File::Silo->CUR_COUNT+1] = 1;

    # is ( $rs->fetch( $id1 ), "NEW STOW AND MAYBE IT WILL GO"x$times, "in trans with correct value." );
    # is ( $rs->fetch( $id2 ), undef, "deleted in trans" );
    # is ( $rs->fetch( $id3 ), "CHANGING THIS SOME", "other changed val in trans");

    # eval {
    #     $rs->rollback_transaction;
    #     fail( "able to rollback without the monkeypatched die" );
    # };
    # like ($@, qr/Breakpoint/, 'break msg' );
    # undef $@;
    # eval {
    #     my $res = $rs->fetch( $id1 );
    #     fail( "Able to get result from bad transaction" );
    # };
    # if( $@ ) {
    #     like ($@, qr/Transaction is in a bad state/, 'bad state errm' );
    #     undef $@;
    # }
    # # test if the state of the recordstore fixes itself with transaction in bad state
    # is ( $copy->fetch( $id1 ), "THIS IS THE FIRST ONE IN THE FIRST PLACE", "id 1 unchanged" );
    # is ( $copy->fetch( $id2 ), "DELETE ME OR TRY TO", "id 2 unchanged" );
    # is ( $copy->fetch( $id3 ), "OH, CHANGE THIS REALLY BIG THING"x$times, "id 3 unchanged" );

    # $dir = tempdir( CLEANUP => 1 );
    # my $store = Yote::RecordStore::File->open_store( $dir );
    # $id = $store->stow( "FOO" );
    # $store->stow( "BAR" );
    # $id2 = $store->stow( "DELME" );
    # $store->use_transaction;
    # $store->delete_record( $id2 );
    # is ( $store->silos_entry_count, 3, '3 entries in silos with commit with last delete' );
    # $store->commit_transaction;
    # is ( $store->silos_entry_count, 2, '2 entries remain in silos after commit w last delete' );
    # $store->delete_record( $id );
    # is ( $store->silos_entry_count, 1, 'now just one entry after commit with last delete' );

    # $dir = tempdir( CLEANUP => 1 );
    # $store = Yote::RecordStore::File->open_store( $dir );
    # $id = $store->stow( "FOO" );
    # $store->stow( "BAR" );
    # $id2 = $store->stow( "DELME" );

    # $store = Yote::RecordStore::File->open_store( $dir );
    # $store->use_transaction;
    # $store->delete_record( $id2 );
    # is ( $store->silos_entry_count, 3, '3 entries in silos with commit with last delete' );

    # no strict 'refs';
    # no warnings 'redefine';

    # local *Yote::RecordStore::File::_vacate = sub {
    #     my( $self, $silo_id, $id_to_empty ) = @_;
    #     if ( $silo_id == 12 && $id_to_empty == $id2 ) {
    #         $self->_unlock;
    #         die "VACATE BREAK";
    #     }
    #     my $silo = $self->[Yote::RecordStore::File->SILOS][$silo_id];
    #     my $rc = $silo->entry_count;
    #     if ( $id_to_empty == $rc ) {
    #         $silo->pop;
    #     } else {
    #         while ( $rc > $id_to_empty ) {
    #             my( $state, $id ) = (@{$silo->get_record( $rc, 'IL' )});
    #             if ( $state == Yote::RecordStore::File->RS_ACTIVE ) {
    #                 $silo->copy_record($rc,$id_to_empty);
    #                 $self->[Yote::RecordStore::File->INDEX_SILO]->put_record( $id, [$silo_id,$id_to_empty], "IL" );
    #                 $silo->pop;
    #                 return;
    #             } elsif ( $state == Yote::RecordStore::File->RS_DEAD ) {
    #                 $silo->pop;
    #             } else {
    #                 return;
    #             }
    #             $rc--;
    #         }
    #     }
    # };
    # no strict 'refs';
    # failnice( $store->commit_transaction,
    #           'VACATE BREAK',
    #           "was able to commit without breakage" );

    # $store = Yote::RecordStore::File->open_store( $dir );
    # is ( $store->silos_entry_count, 3, 'still 3 entries in silos after commit' );

    # $store->fetch( $id, 'FOO', 'reopen store still foos' );
    # is ( $store->silos_entry_count, 3, 'foo doesnt fix the interrupted commit because the interrupted commit was marked complete and just had a few things not purged from the record store ' );
    # $store->delete_record( $id );
    # is ( $store->silos_entry_count, 1, 'back down to bar' );

    {
        $dir = tempdir( CLEANUP => 1 );
        my $store = Yote::RecordStore::File->open_store( $dir );

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
            local *Yote::RecordStore::File::_vacate = sub {
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

} #test_transactions


sub test_sillystrings {

    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );
    locks $store;
    my $packed = pack( "I*", (0..100) );
    is ( $store->stow( $packed ), 1, "id 1 for stowing silly" );
    is ( $store->fetch(1), $packed, "packed string worked" );

} #test_sillystrings


sub test_meta {
    my $dir = tempdir( CLEANUP => 1 );
    my $store = Yote::RecordStore::File->open_store( $dir );

    locks $store;

    failnice ($store->fetch_meta( 3 ),
              'past end of records',
              'no meta to fetch' );
    {
        no warnings 'redefine';
        no strict 'refs';
        
        my $t = 0;

        local *Yote::RecordStore::File::_time = sub {
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
    my $store = Yote::RecordStore::File->open_store($dir);

    return $store;
}
sub reopen {
    my( $cls, $oldstore ) = @_;
    return Yote::RecordStore::File->open_store( $oldstore->[Yote::RecordStore::File->DIRECTORY] );
}

__END__
