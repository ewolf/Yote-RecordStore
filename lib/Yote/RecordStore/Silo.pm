package Yote::RecordStore::Silo;

use strict;
use warnings;
no warnings 'uninitialized';
no warnings 'numeric';
no strict 'refs';

use Carp 'longmess';
use Fcntl qw( SEEK_SET );
use File::Path qw(make_path);

use IO::Handle;

use vars qw($VERSION);
$VERSION = '6.00';

$Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE = 2_000_000_000;
$Yote::RecordStore::Silo::DEFAULT_MIN_FILE_SIZE = 4_096;

use constant {
    DIRECTORY           => 0,
    VERSION             => 1,
    TEMPLATE            => 2,
    RECORD_SIZE         => 3,
    MAX_FILE_SIZE       => 4,
    RECORDS_PER_SUBSILO => 5,
    CUR_COUNT           => 6,
};

sub _die {
    my ($method,$txt) = @_;
    print STDERR Data::Dumper->Dump([longmess]);
    die __PACKAGE__."::$method $txt";
}

sub open_silo {
    my( $class, $dir, $template, $size, $max_file_size ) = @_;

    if( ! $dir ) {
        _die( 'open_silo', "must supply directory to open silo" );
    }
    if( ! $template ) {
        _die( 'open_silo', "must supply template to open silo" );
    }
    my $record_size = $template =~ /\*/ ? $size : do { use bytes; length( pack( $template ) ) };
    if( $record_size < 1 ) {
        _die( 'open_silo', "no record size given to open silo");
    }
    if( $size && $size != $record_size ) {
        _die( 'open_silo', "Silo given size and template size do not match" );
    }

    _make_path( $dir, \my $err );
    if( @$err ) {
        _die( '_make_path', "unable to make $dir directory.". join( ", ", map { $_->{$dir} } @$err ));
    }

    if( $max_file_size < 1 ) {
        $max_file_size = $Yote::RecordStore::Silo::DEFAULT_MAX_FILE_SIZE;
    }

    unless( -e "$dir/0" ) {
        # must have at least an empty silo file
        my $out = _open( '>', "$dir/0" );
        unless ($out) {
            _die( "open_silo", "unable to create silo data file $dir/0 : $@ $!" );
        }
        print $out '';
        close $out;
    }

    return bless [
        $dir,
        $VERSION,
        $template,
        $record_size,
        $max_file_size,
        int($max_file_size / $record_size),
        ], $class;
} #open_silo

sub next_id {
    my( $self ) = @_;
    my $next_id = 1 + $self->entry_count;
    $self->ensure_entry_count( $next_id );
    return $next_id;
} #next_id

sub sync_to_filesystem {
    my $self = shift;

    $self->[CUR_COUNT] = undef;
}

sub entry_count {
    # return how many entries this silo has
    my $self = shift;

    if (defined $self->[CUR_COUNT]) {
        return $self->[CUR_COUNT];
    }

    my @files = $self->subsilos;
    my $filesize;
    for my $file (@files) {
        $filesize += -s "$self->[DIRECTORY]/$file";
    }
    my $count = $self->[CUR_COUNT] = int( $filesize / $self->[RECORD_SIZE] );
} #entry_count

sub get_record {
    my( $self, $id, $template, $offset ) = @_;
    my $rec_size;

    if( $template > 0 ) {
        $rec_size = $template;
        $template = $self->[TEMPLATE];
    } elsif( $template ) {
        my $template_size = $template =~ /\*/ ? 0 : do { use bytes; length( pack( $template ) ) };
        $rec_size = $template_size;
    }
    else {
        $rec_size = $self->[RECORD_SIZE];
        $template = $self->[TEMPLATE];
    }
    if( $id > $self->entry_count || $id < 1 ) {
        _die( 'get_record', "index $id out of bounds for silo $self->[DIRECTORY]. Silo has entry count of ".$self->entry_count );
    }
    my( $idx_in_f, $fh, $subsilo_idx ) = $self->_fh( $id );
  
    $offset //= 0;
    my $seek_pos = ( $self->[RECORD_SIZE] * $idx_in_f ) + $offset;

    sysseek( $fh, $seek_pos, SEEK_SET );
    my $srv = sysread $fh, (my $data), $rec_size;

    return [unpack( $template, $data )];
} #get_record

sub put_record {
    my $self = shift;
    $self->_put_record( @_ );
}

sub _put_record {
    my( $self, $id, $data, $template, $offset ) = @_;
    if( $id > $self->entry_count || $id < 1 ) {
        _die( 'put_record', "index $id out of bounds for silo $self->[DIRECTORY]. Silo has entry count of ".$self->entry_count );
    }
    if( ! $template ) {
        $template = $self->[TEMPLATE];
    }

    my $rec_size = $self->[RECORD_SIZE];
    my $to_write =  pack( $template, ref $data ? @$data : ($data) );

    # allows the put_record to grow the data store by no more than one entry
    my $write_size = do { use bytes; length( $to_write ) };

    if( $write_size > $rec_size) {
        _die( 'put_record', "record size $write_size too large. Max is $rec_size" );
    }

    my( $idx_in_f, $fh, $subsilo_idx ) = $self->_fh( $id );

    $offset //= 0;
    my $seek_pos = $rec_size * $idx_in_f + $offset;
    sysseek( $fh, $seek_pos, SEEK_SET );

    syswrite( $fh, $to_write );

    return 1;
} #put_record

sub pop {
    shift->_pop;
} #pop

sub _pop {
    my( $self ) = @_;
    
    my $entries = $self->entry_count;
    unless( $entries ) {
        return undef;
    }
    my $ret = $self->get_record( $entries );
    my( $idx_in_f, $fh, $subsilo_idx ) = $self->_fh( $entries );

    my $new_subsilo_size = (($entries-1) - ($subsilo_idx * $self->[RECORDS_PER_SUBSILO]  ))*$self->[RECORD_SIZE];

    if( $new_subsilo_size || $subsilo_idx == 0 ) {
        truncate $fh, $new_subsilo_size;
    } else {
        unlink "$self->[DIRECTORY]/$subsilo_idx";
#        FileCache::cacheout_close $fh;
    }
    undef $self->[CUR_COUNT];

    return $ret;
}

sub peek {
    my( $self ) = @_;
    my $entries = $self->entry_count;
    unless( $entries ) {
        return undef;
    }
    my $r = $self->get_record( $entries );
    return $r;
} #peek

sub push {
    my( $self, $data ) = @_;
    my $next_id = $self->next_id;

    $self->put_record( $next_id, $data );

    return $next_id;
} #push



sub record_size { return shift->[RECORD_SIZE] }
sub template { return shift->[TEMPLATE] }

sub max_file_size { return shift->[MAX_FILE_SIZE] }
sub records_per_subsilo { return shift->[RECORDS_PER_SUBSILO] }

sub size {
    # return how many bytes of data this silo has
    my $self = shift;
    my @files = $self->subsilos;
    my $filesize = 0;
    for my $file (@files) {
        $filesize += -s "$self->[DIRECTORY]/$file";
    }
    return $filesize;
}


sub copy_record {
    my( $self, $from_id, $to_id ) = @_;
    my $rec = $self->get_record($from_id);
    $self->put_record( $to_id, $rec );
} #copy_record


#
# Destroys all the data in the silo
#
sub empty_silo {
    my $self = shift;
    my $dir = $self->[DIRECTORY];
    for my $file ($self->subsilos) {
        if( $file eq '0' ) {
            my $fh = _open( '+<', "$dir/0" );
            unless ($fh) {
                _die( "unable to reset silo file $dir/0: $@ $!" );
            }
            truncate $fh, 0;
        } else {
            unlink "$dir/$file";
        }
    }
    $self->[CUR_COUNT] = 0;
    return 1;
} #empty_silo

# destroys the silo. The silo will not be
# functional after this call.
sub unlink_silo {
    my $self = shift;
    my $dir = $self->[DIRECTORY];

    my @subs = $self->subsilos;
    for my $file (@subs) {
        unlink "$dir/$file";
    }
    unlink "$dir/SINFO";
    @$self = ();
} #unlink_silo


#Makes sure this silo has at least as many entries
#as the count given. This creates empty records if needed
#to rearch the target record count.
sub ensure_entry_count {
    my( $self, $count ) = @_;

    my $ec = $self->entry_count;
    my $needed = $count - $ec;
    my $dir = $self->[DIRECTORY];
    my $rec_size = $self->[RECORD_SIZE];
    my $rec_per_subsilo = $self->[RECORDS_PER_SUBSILO];

    if( $needed > 0 ) {
        my( @files ) = $self->subsilos;
        my $write_file = $files[$#files];

        my $existing_file_records = int( (-s "$dir/$write_file" ) / $rec_size );
        my $records_needed_to_fill = $rec_per_subsilo - $existing_file_records;
        $records_needed_to_fill = $needed if $records_needed_to_fill > $needed;
        my $nulls;
        if( $records_needed_to_fill > 0 ) {
            # fill the last file up with \0
#            my $fh = cacheout "+<", 
            my $fh = _open( '+<', "$dir/$write_file" );
            unless ($fh) {
                _die( 'ensure_entry_count', "unable to open $dir/$write_file : $@ $!" );
            }
           # $fh->autoflush(1);
            $nulls = "\0" x ( $records_needed_to_fill * $rec_size );
            my $seek_pos = $rec_size * $existing_file_records;
            sysseek( $fh, $seek_pos, SEEK_SET );
            syswrite( $fh, $nulls );
            close $fh;
            undef $nulls;
            $needed -= $records_needed_to_fill;
        }
        while( $needed > $rec_per_subsilo ) {
            # still needed, so create a new file
            $write_file++;

            if( -e "$dir/$write_file" ) {
                _die( 'ensure_entry_count', "file $dir/$write_file already exists" );
            }
            my $fh = _open( ">", "$dir/$write_file" );
            unless ($fh) {
                _die( 'ensure_entry_count', "could not open file '$dir/$write_file' : $! $@" );
            }
#            $fh->autoflush(1);
            print $fh '';
            unless( $nulls ) {
                $nulls = "\0" x ( $rec_per_subsilo * $rec_size );
            }
            sysseek( $fh, 0, SEEK_SET );
            syswrite( $fh, $nulls );
            $needed -= $rec_per_subsilo;
            close $fh;
        }
        if( $needed > 0 ) {
            # still needed, so create a new file
            $write_file++;

            if( -e "$dir/$write_file" ) {
                _die( 'ensure_entry_count', "file $dir/$write_file already exists" );
            }
            my $fh = _open( ">", "$dir/$write_file" );
            
#            $fh->autoflush(1);
            print $fh '';
            my $nulls = "\0" x ( $needed * $rec_size );
            sysseek( $fh, 0, SEEK_SET );
            syswrite( $fh, $nulls );
            close $fh;
        }
    }
    undef $self->[CUR_COUNT];
    $ec = $self->entry_count;
    return $ec;
} #ensure_entry_count

#
# Returns the list of filenames of the 'silos' of this store. They are numbers starting with 0
#
sub subsilos {
    my $self = shift;
    my $dir = $self->[DIRECTORY];

    _die( 'subsilos', "subsilos called on dead silo" ) unless $dir;

    my $doh = $self->_opensilosdir;

    _die( 'subsilos', "unable to open subsilo directory $dir: $! $@" ) unless $doh;

    my( @files ) = (sort { $a <=> $b } grep { $_ eq '0' || (-s "$dir/$_") > 0 } grep { $_ > 0 || $_ eq '0' } readdir( $doh ) );

    return @files;
} #subsilos


#
# Takes an insertion id and returns
#   an insertion index for in the file
#   filehandle.
#   filepath/filename
#   which number file this is (0 is the first)
#
sub _fh {
    my( $self, $id ) = @_;

    my $dir = $self->[DIRECTORY];
    my $rec_per_subsilo = $self->[RECORDS_PER_SUBSILO];

    my $subsilo_idx = int( ($id-1) / $rec_per_subsilo );
    my $idx_in_f = ($id - ($subsilo_idx*$rec_per_subsilo)) - 1;

    my $fh = _open ("+<", "$dir/$subsilo_idx");
    unless( $fh ) {
        _die( '_fh', "$dir/$subsilo_idx : $!" );
    }
    return $idx_in_f, $fh, $subsilo_idx;

} #_fh

sub _open {
    my( $mod, $file) = @_;
    open my ($fh), $mod, $file;
    return $fh;
}

sub _make_path {
    my( $dir, $err ) = @_;
    make_path( $dir, { error => \$err } );
}

sub _opensilosdir {
    my ($self) = @_;
    my $dir = $self->[DIRECTORY];
    my $dih;
    opendir $dih, $dir;
    return $dih;
}

"Silos are the great hidden constant of the industrialised world.
    - John Darnielle, Universal Harvester";

__END__

=head1 NAME

 Yote::RecordStore::Silo - Indexed Fixed Record Store

=head1 SYNPOSIS

 use Yote::RecordStore::Silo;

 my $silo = Yote::RecordStore->open_silo( $directory, $template, $record_size, $max_file_size );

 my $id = $silo->next_id;
 $silo->put_record( $id, [ 2234234324234, 42, "THIS IS SOME TEXT" ] );

 my $record = $silo->get_record( $id );
 my( $long_val, $int_val, $text ) = @$record;

 my $count = $silo->entry_count;

 my $next_id = $silo->push( [ 999999, 12, "LIKE A STACK" ] );

 my $newcount = $silo->entry_count;
 $newcount == $count + 1;

 $record = $silo->peek;

 $newcount == $silo->entry_count;

 $record = $silo->pop;
 my $newestcount = $silo->entry_count;
 $newestcount == $newcount - 1;

=head1 DESCRIPTION

=head1 METHODS

=head2 open_silo( directory, template, record_size, max_file_size )

=head2 next_id

=head2 entry_count

=head2 get_record

=head2 put_record

=head2 pop

=head2 peek

=head2 push

=head2 sync_to_filesystem 

force this silo to scan the filesystem to get current counts.

=head2 copy_record

=head2 empty_silo

=head2 unlink_silo

=head2 record_size

=head2 template

=head2 max_file_size

=head2 records_per_subsilo

=head2 size

=head2 ensure_entry_count

=head2 subsilos

=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2012 - 2019 Eric Wolf. All rights reserved.  This program is free software; you can redistribute it and/or modify it
       under the same terms as Perl itself.

=head1 VERSION
       Version 6.00  (Oct, 2019))

=cut


= Timing experiment using cached CUR_COUNT =================

---------- with CUR_COUNT --------------

Done saving

real	9m12.277s
user	9m6.004s
sys	0m5.768s

---------- withOUT CUR_COUNT --------------

so a tiny bit, maybe not worth it?

Done saving

real	9m27.667s
user	9m21.591s
sys	0m5.772s
