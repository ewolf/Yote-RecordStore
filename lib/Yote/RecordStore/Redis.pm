package Yote::RecordStore::Redis;

use 5.16.0;
use warnings;

use Data::Dumper;
use Redis;

use Fcntl qw( :flock SEEK_SET );
use File::Path qw(make_path);
use Scalar::Util qw(openhandle);


my $server = "127.0.0.1:6379";

sub open_store {
    my ($pkg, $dir) = @_;
    my $masterkey = "${dir}:__MASTER__";
    my $idkey = "${dir}:__LASTID__";
    my $redis = Redis->new(
        server => $server,
        name   => "RecordStore: $dir",
        );


    unless( -d $dir ) {
        _make_path( $dir, \my $err, 'base' );
        if (@$err) {
            _err( 'open_store', "unable to make base directory.". join( ", ", map { $_->{$dir} } @$err ) );
        }
    }

    my $lockfile = "$dir/LOCK";
    my $lock_fh;

    # be locked, error out here
    if (-e $lockfile) {
        $lock_fh = _open ($lockfile, '>' );
        unless (_flock( $lock_fh, LOCK_EX) ) {
            die "cannot open, unable to open lock file '$lockfile' to open store: $! $@";
        }
    } else {
        $lock_fh = _open ($lockfile, '>' );
        unless ($lock_fh = _open ($lockfile, '>' )) {
            die "cannot open, unable to open lock file '$lockfile' to open store: $! $@";
        }
        $lock_fh->autoflush(1);
        unless (_flock( $lock_fh, LOCK_EX) ) {
            die "cannot open, unable to open lock file '$lockfile' to open store: $! $@";
        }
        print $lock_fh "LOCK";
    }
    
    my $store = bless {
        name  => $dir,
        masterkey => $masterkey,
        idkey => $idkey,
        redis => $redis,
        LOCK_FH => $lock_fh,
        LOCK_FILE => $lockfile,
    }, $pkg;

    return $store;
}

=item is_locked

returns true if this recordstore is currntly locked.

=cut
sub is_locked {
    my $self = shift;
    return $self->{IS_LOCKED};
}

sub _make_path {
    my( $dir, $err, $msg ) = @_;
    make_path( $dir, { error => $err } );
}


sub _openhandle {
    return openhandle( shift );
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

sub _flock {
    my ($fh, $flags) = @_;
    return $fh && flock($fh,$flags);
}

=item lock

lock this recordstore.

=cut
sub lock {
    my $self = shift;

    return 1 if $self->{IS_LOCKED};

    my $lock_fh = _openhandle( $self->{LOCK_FH});
    unless ($lock_fh) {
        unless ($lock_fh = _open ( $self->{LOCK_FILE}, '>' )) {
            die "unable to lock: lock file $self->{LOCK_FILE} : $@ $!";
        }
    }
    $lock_fh->blocking( 1 );
#    $self->_log( "$$ try to lock" );
    unless (_flock( $lock_fh, LOCK_EX )) {
        die "unable to lock: cannot open lock file '$self->{LOCK_FILE}' to open store: $! $@";
    }
#    $self->_log( "$$ locked" );
    $self->{LOCK_FH} = $lock_fh;
    $self->{IS_LOCKED} = 1;

#    $self->_reset;
    return 1;
}

=item unlock

unlock this recordstore.

=cut
sub unlock {
    my $self = shift;

    unless ($self->{IS_LOCKED}) {
        _warn( 'unlock', "store not locked" );
        return 1;
    }

    $self->{IS_LOCKED} = 0;
#    $self->_log( "$$ try to unlock" );
    unless (_flock( $self->{LOCK_FH}, LOCK_UN ) ) {
        _err( "unlock", "unable to unlock $@ $!" );
    }
    $self->{LOCK_FH} && $self->{LOCK_FH}->close;
    undef $self->{LOCK_FH};
#    $self->_log( "$$ unlocked" );
}


# sub lock {
#     my ($self, @keys) = @_;

#     # lock individual keys
#     if (@keys) {
#         #lock master, lock keyed locks, then unlock master
#         $self->lock; 
#         for my $key (@keys) {
#             $key = "^$self->{name}:$key"; # / is invalid in first position for lock
#             $key =~ s!/!_!g;
#             my $mutex = $self->{mutex}{$key} = $self->{distLock}->lock( $key, 1000 ); #1000 seconds ttl
#             $self->{mutex}{$key} = $mutex;
#             _err( 'lock', "could not get distributed lock $! $@" ) unless $mutex;
#             $self->{IS_LOCKED}{$key} = 1;
#         }
#         $self->lock;
#     } 

#     # set the master lock
#     else {
#         return 1 if ($self->{MASTER_LOCKED});
#         my $key = $self->{masterkey};
#         my $distLock = Redis::DistLock->new( 
#             servers => [$server],
#             retry_count => 100,
#             auto_release => 1,
#             );
#         $self->{distLock} = $distLock;
#         my $mutex = $self->{master_mutex} = $distLock->lock( $key, 1000 ); #1000 seconds ttl
#         print STDERR "\n   **LOCK** <$mutex> ($$)\n\n";
#         _err( 'lock', "could not get distributed lock for '$key' $@ $!" ) unless $mutex;
#         $self->{MASTER_LOCKED} = 1;
#     }
#     return 1;
# }

# sub unlock {
#     my ($self, @keys) = @_;
#     if (@keys) {
#         #lock master, unlock keyed locks, then unlock master
#         $self->lock;
#         for my $key (@keys) {
#             $key = "$self->{name}:$key";
#             my $mutex = $self->{mutex}{$key};
#             unless ($mutex) {
#                 _warn( 'unlock', "is not locked by key '$key'" );
#             }
#             $self->{distLock}->release( $mutex );
#             $self->{IS_LOCKED}{$key} = 0;
#         }
#         $self->unlock;
#     } else {
#         my $mutex = $self->{master_mutex};
#         unless ($mutex) {
#             _warn( 'unlock', "is not locked by master key" );
#         } else {
#             print STDERR "\n   **UNLOCK** <$mutex> ($$)\n\n";
#             $self->{distLock}->release( $mutex );
#         }
#         $self->{MASTER_LOCKED} = 0;
#     }
#     return 1;
# }

sub first_id {
    return 1;
}

sub use_transaction {

}

sub commit_transaction {

}

sub stow {
    my ($self, $data, $id) = @_;
    unless ($self->{IS_LOCKED}) {
        _err( 'stow', "record store not locked");
    }
    if ($id == 1 && !$self->{redis}->get( $self->{idkey} ) ) {
        $self->{redis}->set( $self->{idkey}, 1 );
    }
    if (!$id) {
        $id = $self->next_id;
    }

    $id = "$self->{name}:$id";

#    print STDERR "STOW $id --> ".length($data)." bytes\n";

#print STDERR Data::Dumper->Dump([$data,$id,"STOW"]);

    $self->{redis}->set( $id, $data );
}

sub fetch {
    my( $self, $id ) = @_;
    unless ($self->{IS_LOCKED}) {
        _err( 'fetch', "record store not locked");
    }
    $id = "$self->{name}:$id";
    my $val = $self->{redis}->get( $id );
#    print STDERR "FETCH $id --> ".length($val)." bytes\n";
#    print STDERR Data::Dumper->Dump([$val,$id,"FETCH"]);
    return 0, 0, $val;
}

sub next_id {
    my $self = shift;
    unless ($self->{IS_LOCKED}) {
        _err( 'next_id', "record store not locked");
    }
    return $self->{redis}->INCR( $self->{idkey} );
}

sub record_count {
    my $self = shift;
    unless ($self->{IS_LOCKED}) {
        _err( 'record_count', "record store not locked");
    }
    my @keys = $self->{redis}->keys( "*$self->{name}:*" );
    
    return @keys > 2 ? @keys - 2 : 0;
}

sub _err {
    my ($method,$txt) = @_;
use Carp 'longmess'; print STDERR Data::Dumper->Dump([longmess]);
    die __PACKAGE__."::$method $txt";
}

sub _warn {
    my ($method,$txt) = @_;
    warn __PACKAGE__."::$method $txt";
}


1;
