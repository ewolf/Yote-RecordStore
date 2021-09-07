#!/usr/bin/perl
use strict;
use warnings;

use Carp;
use Data::Dumper;
use Errno qw(ENOENT);
use File::Temp qw/ :mktemp tempdir /;
use Test::More;
use Time::HiRes qw(usleep);

use lib 't/lib';
use forker;

#$SIG{ __DIE__ } = sub { Carp::confess( @_ ) };

use lib './lib';
use Yote::RecordStore::File::Silo;

# -----------------------------------------------------
#               init
# -----------------------------------------------------

test_init();
test_use();
test_async();
done_testing;

exit( 0 );

sub failnice {
    my( $subr, $errm, $msg ) = @_;
    is ($subr->(), undef, $msg );
    like( $@, qr/$errm/, "$msg error" );
    undef $@;
}

sub test_init {
    my $dir = tempdir( CLEANUP => 1 );
    my $size = 2 ** 10;
    my $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LZ*', $size );
    ok( $silo, "Got a silo" );
    my $new_silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LZ*', $size );
    ok( $new_silo, "able to renit already inited silo with same params" );

    failnice( sub { Yote::RecordStore::File::Silo->open_silo( $dir, 'LZ*' ) },
              "no record size given to open silo",
              "was able to reinit silo withthout specifying record size" );
    failnice( sub { Yote::RecordStore::File::Silo->open_silo( $dir, undef, 100 ) },
              "must supply template to open silo",
              "was able to reinit silo withthout specifying template" );

    failnice( sub { Yote::RecordStore::File::Silo->open_silo() },
              "must supply directory to open silo",
              "was able to reinit silo withthout specifying dir" );

    $dir = tempdir( CLEANUP => 1 );

    failnice( sub { Yote::RecordStore::File::Silo->open_silo( $dir, 'LLL', 800 ) },
              'do not match',
              'template size and given size do not match' );
    
    
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LLL' );
    is( $silo->template, 'LLL', 'given template matches' );

    is( $silo->max_file_size, 2_000_000_000, "silo is default max size" );
    is( $silo->record_size, 12, "silo has 32 bytes per record" );
    is( $silo->records_per_subsilo, 166_666_666, "166,666,666 records per file" );

    {
        no strict 'refs';
        no warnings 'redefine';

        $dir = tempdir( CLEANUP => 1 );
        my $cantdir = "$dir/cant";

        local *Yote::RecordStore::File::Silo::_make_path = sub {
            my ($dir,$err) = @_;
            if ($dir =~ /cant$/) {
                $$err = [$dir];
                return undef;
            }
            make_path( $dir, { error => \$err } );
        };

        failnice( sub { Yote::RecordStore::File::Silo->open_silo( $cantdir, 'LL' ) },
                  "unable to make",
                  'was able to init a silo in an unwritable directory' );
     }
    $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE = 2_000_000_000;
} #test_init

sub test_use {
    my $dir = tempdir( CLEANUP => 1 );
    my $size = 2 ** 10;
    my $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size, $size * 10, );
    is( $silo->size, 0, 'nothing in the silo, no size' );
    is( $silo->entry_count, 0, 'nothing in the silo, no entries' );
    is_deeply( [$silo->subsilos], [0], 'one subsilo upon creation' );

    is( $silo->pop, undef, "nothing to pop" );
    is( $silo->peek, undef, "nothing to peek" );

    is( $silo->size, 0, 'nothing in the silo, no size still' );
    is( $silo->entry_count, 0, 'nothing in the silo, no entries still' );
    is_deeply( [$silo->subsilos], [0], 'one subsilo upon creation still' );
    
    $silo->ensure_entry_count( 1 );
    is( $silo->size, $size, 'silo now with one record. correct size' );
    is( $silo->entry_count, 1, 'silo now with one record. correct count' );
    is ($silo->get_record(0), undef, 'record zero');
    like ($@, qr/index 0 out of bounds/, 'record zero error message' );
    is ($silo->get_record(2), undef, 'record out of bounds');
    like ($@, qr/index 2 out of bounds/, 'record out of bounds error message' );
    is_deeply( $silo->get_record( 1 ), [''], 'empty record' );
    is_deeply( $silo->peek, [''], "empty peek" );

    $silo->pop;
    is( $silo->size, 0, 'nothing in the silo, no size after pop' );
    is( $silo->entry_count, 0, 'nothing in the silo, no entries after pop' );
    is_deeply( [$silo->subsilos], [0], 'one subsilo upon creation after pop' );
    $silo->ensure_entry_count( 1 );

    
    is( $silo->next_id, 2, "next id" );

    is( $silo->size, 2*$size, 'silo now with two. correct size' );
    is( $silo->entry_count, 2, 'silo now with two. correct count' );

    $silo->ensure_entry_count( 12 );
    is( $silo->size, 12*$size, 'silo now with 12. correct size' );
    is( $silo->entry_count, 12, 'silo now with 12. correct count' );
    is_deeply( [$silo->subsilos], [0,1], 'one subsilo upon creation' );

    is_deeply( $silo->pop, [''], 'empty popped record' );
    is( $silo->size, 11*$size, 'silo now with 11. correct size after pop one' );
    is( $silo->entry_count, 11, 'silo now with 11. correct count after pop one' );
    is_deeply( [$silo->subsilos], [0, 1], 'same subsilos after pop one' );
    
    is_deeply( $silo->pop, [''], 'empty popped record' );
    is( $silo->size, 10*$size, 'silo back to 10. correct size ' );
    is( $silo->entry_count, 10, 'silo back to 10. correct count' );
    is_deeply( [$silo->subsilos], [0], 'one less subsilo after pop two' );

    $silo->ensure_entry_count( 40 );
    is( $silo->size, 40*$size, 'silo to 40. correct size ' );
    is( $silo->entry_count, 40, 'silo to 40. correct count' );
    is_deeply( [$silo->subsilos], [0,1,2,3], 'four subsilos after 40 entries' );

    $silo->ensure_entry_count( 30 );
    is( $silo->size, 40*$size, 'silo still 40. correct size ' );
    is( $silo->entry_count, 40, 'silo still 40. correct count' );
    is_deeply( [$silo->subsilos], [0,1,2,3], 'four subsilos still 40 entries' );

    ok( $silo->put_record( 10, ["BLBLBLBLBLBL"] ), "put a record" );
    is( $silo->size, 40*$size, 'silo still 40. correct size after put' );
    is( $silo->entry_count, 40, 'silo still 40. correct count after put' );
    is_deeply( [$silo->subsilos], [0,1,2,3], 'four subsilos still 40 entries after put' );

    is_deeply( $silo->get_record( 10 ), [ "BLBLBLBLBLBL" ], "record was created" );
    is_deeply( $silo->get_record( 9 ), [''], "empty 9" );
    is_deeply( $silo->get_record( 11 ), [''], "empty 11" );

    is( $silo->push( "UUUUUUUU" ), 41, "pushed with id 41" );
    is( $silo->size, 41*$size, 'silo pushed to 41. correct size after put' );
    is( $silo->entry_count, 41, 'silo pushed to 41. correct count' );
    is_deeply( [$silo->subsilos], [0,1,2,3,4], 'five subsilos for 41 entries' );
    
    is_deeply( $silo->peek, [ 'UUUUUUUU' ], 'last pushed record' );
    is_deeply( $silo->pop, [ 'UUUUUUUU' ], 'last pushed record' );

    is( $silo->size, 40*$size, 'silo still 40. correct size after pop' );
    is( $silo->entry_count, 40, 'silo still 40. correct count after pop' );
    is_deeply( [$silo->subsilos], [0,1,2,3], 'four subsilos still 40 entries after pop' );

    is ( $silo->put_record( 41, "WRONG" ), undef, 'record beyond end of bounds' );
    like( $@, qr/out of bounds/, 'error message for put past entries' );
    
    is ($silo->put_record( 0, "WRONG" ), undef, 'put record with index of 0' );

    like( $@, qr/out of bounds/, 'error message for zero index' );
    
    is ($silo->put_record( -1, "WRONG" ), undef, 'put record with index < 0' );
    like( $@, qr/index -1 out of bounds/, 'error message for wrong index' );

    is ($silo->put_record( 5, "WRONG".('x'x$size) ), undef, 'put record too big' );
    like( $@, qr/too large/, 'error message for too big data' );

    {

        $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size, $size * 10 );

        {
            no strict 'refs';
            no warnings 'redefine';
            local *Yote::RecordStore::File::Silo::_opensilosdir = sub {
                $@ = 'monkeypatch';
                return undef;
            };
            
            failnice( sub { $silo->subsilos },
                      "unable to open subsilo directory $dir.*monkey",
                      "Was able to access subsilos despite dark directory",
                );
        }


        is_deeply( [$silo->subsilos], [ 0, 1,2,3], "still four subsilos after open" );

        # {
        #     chmod 0444, "$dir/3";
        #     eval {
        #         $silo->peek;
        #     };
        #     like( $@, qr/Unable to open|Permission denied/, 'error msg for readonly file' );
        # }
        # chmod 0777, "$dir/3";

        $silo->put_record(40,'LAST');
        is_deeply( $silo->peek, ['LAST'], 'last is last' );
        
        $silo->put_record(1,'FIRST');
        is_deeply( $silo->get_record(1), ['FIRST'], 'first is first' );

        $silo->put_record(1,'FIR');
        is_deeply( $silo->get_record(1), ['FIR'], 'fir is first' );
        
        $silo->put_record(1,'FIRST');
        is_deeply( $silo->get_record(1), ['FIRST'], 'first is again first' );
        
        $silo->put_record(1,'');
        is_deeply( $silo->get_record(1), [''], 'empty is first' );
        
        $silo->put_record(1,'F');
        is_deeply( $silo->get_record(1), ['F'], 'f is first' );

        $silo->empty_silo;

        is_deeply( [$silo->peek], [undef], 'nothing to peek at after empty silo' );
        is_deeply( [$silo->subsilos], [ 0], "empty only has first subsilo " );

        is ($silo->entry_count, 0, 'no entries in emtpy' );

        open my $fh, '>', "$dir/3";
        print $fh '';
        close $fh;
        failnice( sub { $silo->ensure_entry_count( 40 ) },
                   '3 already exists',
                   'able to ensure count with wacky extra subsilo 3 hanging out' );

        is ($silo->entry_count, 0, 'still no entries in emtpy' );

        open $fh, '>', "$dir/2";
        print $fh '';
        close $fh;
        failnice( sub { $silo->ensure_entry_count( 40 ) },
                   '2 already exists',
                   'able to ensure count with wacky extra subsilo 2 hanging out' );

        is ($silo->entry_count, 0, 'even still no entries in emtpy' );

        $silo->empty_silo;

        {
            no strict 'refs';
            no warnings 'redefine';
            local *Yote::RecordStore::File::Silo::_open = sub {
                my( $mod, $file) = @_;
                if ($file eq "$dir/0") {
                    $@ = 'monkeypatch';
                    return undef;
                }
                open my ($fh), $mod, $file;
                return $fh;
            };

            failnice( sub { $silo->ensure_entry_count( 40 ) },
                      'unable to open.*monkey',
                      'able to ensure count with wacky extra subsilo hanging out' );
            # eval {
            #     $silo->ensure_entry_count( 3 );
            #     fail( 'able to ensure count with unwriteable first' );
            # };
        }
        
    }

    $silo->unlink_silo;

    is_deeply( [$silo->subsilos], [], "no subsilos after unlink silo" );

    $dir = tempdir( CLEANUP => 1 );
    $size = 2 ** 10;
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LIZ*', $size, $size * 10 );
    my $id = $silo->next_id;
    is_deeply( $silo->get_record(1), [0,0,''], 'starting with nothing' );
    $silo->put_record( $id, [12,8,"FOOFOO"] );
    is_deeply( $silo->get_record(1), [12,8,'FOOFOO'], 'starting with 12, FOOFOO' );
    $silo->put_record( $id, [42], 'L' );
    is_deeply( $silo->get_record(1), [42,8,'FOOFOO'], 'starting with FOOFOO but adjusted 12 --> 42 ' );
    is_deeply( $silo->get_record(1,'L'), [42], 'just the 42 ' );

    $silo->put_record( $id, [333], 'I', 4 );
    is_deeply( $silo->get_record(1), [42,333,'FOOFOO'], 'starting with FOOFOO but adjusted 8 --> 333 ' );
    is_deeply( $silo->get_record(1, 'L', 0 ), [42], 'picking out 333 ' );
    is_deeply( $silo->get_record(1, 'I', 4 ), [333], 'picking out  333 ' );

    $dir = tempdir( CLEANUP => 1 );
    $size = 2 ** 10;
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size );
    $id = $silo->push( "BARFY" );
    is_deeply( $silo->get_record($id), ['BARFY'], 'got record after single item on' );
    $silo->put_record( $id, "BARFYYY", "Z*" );
    is_deeply( $silo->get_record($id), ['BARFYYY'], 'got record after single item on with put record' );
    $silo->put_record( $id, ["BARFYYYZ"], "Z*" );
    is_deeply( $silo->get_record($id), ['BARFYYYZ'], 'got record after array item plus template on with put record' );

    is_deeply( $silo->get_record($id,"Z*"), [''], 'star template doesnt work with get_record. must use size ' );
    is_deeply( $silo->get_record($id,4), ['BARF'], 'use size rather than template for get_record ' );

    $dir = tempdir( CLEANUP => 1 );
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LIL' );
    $silo->push( [234,4,6654] );
    is_deeply( $silo->get_record( 1, 'LI' ), [234,4], 'get front part' );
    is_deeply( $silo->get_record( 1, 'I', 4 ), [4], 'get second' );

    
    $Yote::RecordStore::File::Silo::DEFAULT_MAX_FILE_SIZE = 2_000_000_000;

    # test copy numbers
    $dir = tempdir( CLEANUP => 1 );
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'LL' );
    $silo->push( [ 3, 56 ] );
    $id = $silo->next_id;
    $silo->copy_record( 1, 2 );
    is_deeply( $silo->get_record(2), [3,56], 'copied numbers' );
    
    
    # test copy
    $dir = tempdir( CLEANUP => 1 );
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', 2 ** 12 );
    $id = $silo->push( "SOMETHING EXCELLENT" );
    is ($id, 1, 'firsty id');
    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'first value' );
    is ($silo->copy_record( 1, 0 ), undef, 'copy to zero dest' );
    like ($@, qr/out of bounds/, 'copy to zero dest error message' );
    undef $@;

    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'first value 1' );
              
    is ( $silo->copy_record( 0, 1 ), undef, 'copy from zero source' );
    like ($@, qr/out of bounds/, 'copy from zero source message' );
    undef $@;

    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'first value 2' );

    is ( $silo->copy_record( 3, 1 ), undef, 'copy from too big source' );
    like ($@, qr/out of bounds/, 'copy from too big source message' );
    undef $@;
              
    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'first value 3' );

    is( $silo->copy_record( 1, 3 ), undef, 'copy to too big dest' );
    like ($@, qr/out of bounds/, 'copy to too big dest message' );
    undef $@;

    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'first value 4' );
              
    $id = $silo->next_id;
    is ($id,2,"next id given");
    is ($silo->entry_count,2,'two entries');
    is_deeply( $silo->get_record(2), [''], 'nothing for new next id' );
    $silo->copy_record( 1, 2 );

    is_deeply( $silo->get_record(2), ['SOMETHING EXCELLENT'], 'copy worked for new id' );
    is_deeply( $silo->get_record(1), ['SOMETHING EXCELLENT'], 'original still there' );

    $size = 2 ** 10;
    $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size, $size * 10, );
    is ($silo->push( "WRONG".('x'x$size) ), undef, 'push record too big' );
    like( $@, qr/too large/, 'error message for too big data push' );

    {
        my $dir2 = tempdir( CLEANUP => 1 );
        my $silo2 = Yote::RecordStore::File::Silo->open_silo( $dir2, 'Z*', $size, $size * 10, );
        is ($silo2->push( "TEST" ), 1, "able to insert test" );
        is_deeply( $silo2->get_record(1), ['TEST'], 'put record in simple silo' );

        my $trip = 1;
        no strict 'refs';
        no warnings 'redefine';
        local *Yote::RecordStore::File::Silo::_open = sub {
            if ($trip++ > 0) {
                $@ = 'monkeypatch';
                return undef;
            }
            my( $mod, $file) = @_;
            open my ($fh), $mod, $file;
            return $fh;
            
        };
        
        failnice ( sub { $silo->empty_silo },
                   'unable to reset.*monkey', 
                   'unable to empty silo due to monkeypatch' );

        $dir = tempdir( CLEANUP => 1 );
        failnice( sub { $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size, $size * 10, ); },
                  'unable to create silo data file',
                  'file wont open so open_silo fails' );

        failnice( sub { $silo2->get_record(1); },
                  'Unable to open subsilo',
                  'get record not able to open file' );

        $trip = 0;
        failnice( sub { $silo2->ensure_entry_count(10000); },
                  'could not open file',
                  'get record not able to open file' );
    }


} #test_use

sub test_async {
    
    my $dir = tempdir( CLEANUP => 1 );
    my $forker = forker->new( $dir );
    my $size = 2 ** 10;
    my $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size );
    
    $forker->init();

    my $A = fork;    
    unless( $A ) {
        $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size );
        $forker->expect( '1' );
        usleep( 5000 );
        my $id = $silo->next_id;
        $forker->put( "ID A $id" );
        exit;
    }

    my $B = fork;    
    unless( $B ) {
        $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size );
        $forker->spush( '1' );
        $forker->expect( "ID A 1" );
        my $id = $silo->push( "SOUPS" );
        $forker->put( "ID B 2" );
        exit;
    }

    my $C = fork;    
    unless( $C ) {
        $silo = Yote::RecordStore::File::Silo->open_silo( $dir, 'Z*', $size );
        $forker->spush( '1' );
        $forker->spush( "ID A 1" );
        $forker->expect( "ID B 2" );
        my $val = $silo->get_record( 2 );
        $forker->put( "VAL $val->[0]" );
        exit;
    }
    $forker->put( '1' );
    
    waitpid $A, 0;
    waitpid $B, 0;
    waitpid $C, 0;

    is_deeply( $forker->get, [ '1', 'ID A 1' , 'ID B 2', 'VAL SOUPS' ], "correct order for things" );

    
} #test_async
