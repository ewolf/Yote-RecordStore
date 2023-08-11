package forker;

use v5.10;

use strict;
use warnings;

use Fcntl qw( :flock );
use Time::HiRes qw(usleep);

sub new {
    my( $cls, $dir ) = @_;
    my $forker = bless {
        sema  => "$dir/sema", #semaphore
        stack => [],
    }, $cls;
    $forker->init;
    return $forker;
}

sub init {
    my $self = shift;
    open my $out, ">", $self->{sema};
    $out->blocking(1);
    print $out "";
    $self->{stack} = [];
}

sub spush {
    my( $self, @stuff ) = @_;
    push @{$self->{stack}}, @stuff;
}

sub put {
    my( $self, $txt ) = @_;
    $self->spush( $txt );
    open my $out, ">>", $self->{sema} or die "$! $@";
    $out->blocking(1);
    #print STDERR "FORKER PUT FLOCK $$\n";
    flock( $out, LOCK_EX );
    print $out "$txt\n";
    #print STDERR "FORKER PUT UNFLOCK $$\n";
    flock( $out, LOCK_UN );
    close $out;
}

sub get {
    my $self = shift;
    my $out = [];
    open my $in, '<', $self->{sema} or die "$! $@";
    $in->blocking(1);
    #print STDERR "FORKER GET FLOCK $$\n";
    flock( $in, LOCK_SH );
    while( <$in> ) {
        chop;
        push( @$out, $_ ) if $_;
    }
    #print STDERR "FORKER GET UNFLOCK $$\n";
    flock( $in, LOCK_UN );
    return $out;
}

sub expect {
    my( $self, $what, $tag ) = @_;
    $self->spush( $what );
    while ( 1 ) {
        my @got      = @{$self->get()};
        my @expected = @{$self->{stack}};
#say "$tag: ".join(",", @got). ' eq '. join(",", @expected);
        if (grep { $_ eq '__DEATH__' } @got) {
            exit;
        }
        if( join(",", @got) eq join(",", @expected) ) {
            return;
        }
        if (@got > @expected) {
            $self->put( '__DEATH__' );
            exit;
        }
        usleep(500);
    }
}

"when you come to a fork in the road, take it - Yogi Berra";
