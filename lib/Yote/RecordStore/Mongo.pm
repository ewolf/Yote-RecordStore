package Yote::RecordStore::Mongo;

use strict;
use warnings;
no warnings 'uninitialized';

use Data::Dumper;
use Time::HiRes;

use BSON::OID;
use MongoDB;

=head1 NAME

 Yote::RecordStore - Simple store for text and byte data

=head1 SYNPOSIS

 use Yote::RecordStore;

 $store = Yote::RecordStore->init_store( DIRECTORY => $directory, MAX_FILE_SIZE => 20_000_000_000 );
 $data = "TEXT OR BYTES";

 # the first record id is 1
 my $id = $store->stow( $data );

 my $val = $store->fetch( $some_id );

 my $count = $store->entry_count;

 $store->lock( qw( FEE FIE FOE FUM ) ); # lock blocks, may not be called until unlock.

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

=head2 open_store( $options )



=cut

sub open_store {
    my( $cls, %options ) = @_;

    # ------------ connect to db ------------
    my ($host, $db_name) = ($options{host} || 'localhost',
                            $options{db_name} || 'yote');
    my $client = MongoDB->connect("mongodb://$host", \%options);
    my $objs = $client->ns("$db_name.objects");
    my $root = $client->ns("$db_name.root");

    my $store =	bless {
        options => {%options},
        objs    => $objs,
        root    => $root,
    }, $cls;
    
    return $store;
} #open_store

sub lock {}
sub unlock {}

=head2 close_store()



=cut

sub close_recordstore {
    #noop
}

sub root_id {
    my $self = shift;
    my $root = $self->{root}->find_one();
    if ($root) {
        return $root->{id};
    }
    my $root_id = $self->next_id;
    $root = { id => $root_id };
    $self->{root}->insert_one( $root );
    return $root->{id};
}


=head2 get_record_count()



=cut

sub get_record_count {
    my $self = shift;
    return $self->{objs}->count_documents({});
}

=head2 fetch($id)



=cut

sub fetch {
    my( $self, $id ) = @_;
    my $obj = $self->{objs}->find_one( { _id => $id } );
    return ($obj->{created}, $obj->{updated}, $obj->{data});
} #fetch

=head2 stow($data, $id)



=cut

sub stow {
    my( $self, $string_data, $id ) = @_;

    my $data = { data => $string_data };

    $id //= $self->next_id;

    my $oldo = $self->{objs}->find_one( { _id => $id } );
    if ($oldo) {
        $data->{created} = $oldo->{created};
        $self->{objs}->delete_one( { _id => $id } );
    } else {
        $data->{created} = time;
    }
    $data->{updated} = time;
    $self->{objs}->insert_one( { _id => $id, %$data } );

    return $id;
} #stow

=head2 next_id()



=cut

sub next_id {
    return BSON::OID->new;
} #next_id

=head2 delete_record( $del_id )



=cut

sub delete_record {
    my( $self, $del_id ) = @_;
    $self->{objs}->delete_one( { _id => $del_id } );
} #delete_record
=head2 use_transaction()



=cut

sub use_transaction {
    return 1;
} #use_transaction

=head2 commit_transaction()



=cut

sub commit_transaction {
    return 1;
} #commit_transaction

=head2 rollback_transaction()



=cut

sub rollback_transaction {
    return 1;
} #rollback_transaction

=head2 detect_version()



=cut

sub detect_version {

} #detect_version



"I became Iggy because I had a sadistic boss at a record store. I'd been in a band called the Iguanas. And when this boss wanted to embarrass and demean me, he'd say, 'Iggy, get me a coffee, light.' - Iggy Pop";


