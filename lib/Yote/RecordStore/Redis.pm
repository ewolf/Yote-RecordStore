package Yote::RecordStore::Redis;

use 5.16.0;
use warnings;

use Data::Dumper;
use Redis;

use constant {
    NAME        => 0,
    REDIS       => 1,
    LOCKER      => 2,
    IDKEY       => 3,
    TRANSACTION => 4,
};

sub open_store {
    my ($pkg, %args) = @_;

    my $name = $args{name};
    die "Needs name" unless $name;
    my $locker = $args{locker};
    die "Needs locker" unless $locker;

    my $redis = Redis->new(
        server => $args{redis_ip} || '127.0.0.1:6379',
        name   => "RecordStore: $name",
        );

    my $idkey = "${name}:__LASTID__";

    my $store = bless [
        $name,
        $redis,
        $locker,
        $idkey,
    ], $pkg;

    return $store;
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



sub first_id {
    return 1;
}

sub use_transaction {

}

sub commit_transaction {

}

sub stow {
    my ($self, $data, $id) = @_;
    unless ($self->is_locked) {
        _err( 'stow', "record store not locked");
    }
    if ($id == 1 && !$self->[REDIS]->get( $self->[IDKEY] ) ) {
        $self->[REDIS]->set( $self->[IDKEY], 1 );
    }
    if (!$id) {
        $id = $self->next_id;
    }

    $id = "$self->[NAME]:$id";

#    print STDERR "STOW $id --> ".length($data)." bytes\n";

#print STDERR Data::Dumper->Dump([$data,$id,"STOW"]);

    $self->[REDIS]->set( $id, $data );
}

sub fetch {
    my( $self, $id ) = @_;
    unless ($self->is_locked) {
        _err( 'fetch', "record store not locked");
    }
    $id = "$self->[NAME]:$id";
    my $val = $self->[REDIS]->get( $id );
#    print STDERR "FETCH $id --> ".length($val)." bytes\n";
#    print STDERR Data::Dumper->Dump([$val,$id,"FETCH"]);
    return 0, 0, $val;
}

sub next_id {
    my $self = shift;
    unless ($self->is_locked) {
        _err( 'next_id', "record store not locked");
    }
    return $self->[REDIS]->INCR( $self->[IDKEY] );
}

sub record_count {
    my $self = shift;
    unless ($self->is_locked) {
        _err( 'record_count', "record store not locked");
    }
    my $count = $self->[REDIS]->get( $self->[IDKEY] );
    return $count || 0;
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

sub _reset {

}

1;
