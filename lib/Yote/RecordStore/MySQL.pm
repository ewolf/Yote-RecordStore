package Yote::RecordStore::MySQL;

use strict;
use warnings;
no warnings 'uninitialized';

use Data::Dumper;
use DBI;

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

    unless ($options{database}) {
	$@ = "no database selected";
	return undef;
    }

    # ------------ connect to db ------------
    my $dsn = 'DBI:mysql:'.join( ':', map { "$_=$options{$_}" } grep { $options{$_} } qw( database host port ) );
    my $dbh = DBI->connect( $dsn, $options{user}, $options{password}, \%options );
    unless ($dbh) {
	# maybe database has not been created
	$dsn = 'DBI:mysql:'.join( ':', map { "$_=$options{$_}" } grep { $options{$_} } qw( host port ) );
	$dbh = DBI->connect( $dsn, $options{user}, $options{password}, \%options );
	unless ($dbh) {
	    $@ = "could not connect to database";
	    return undef;
	}
	my $db = $options{database};
	$db =~ s/[^a-zA-Z0-9_]//g; #remove non alphanumerics
	unless ($db) {
	    $@ = "could not create database with name '$options{database}'";
	    return undef;
	}
	unless (defined $dbh->do( "CREATE DATABASE IF NOT EXISTS $db" )) {
	    $@ = "could not create database : ". $dbh->errstr;
	    return undef;
	}
    }
    $dbh->{mysql_auto_reconnect} = 1;
    
    # ------------ create recordstore table ------------
    unless (defined $dbh->do( "CREATE TABLE IF NOT EXISTS recordstore (
			         id BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
			         created bigint,
			         updated bigint,
                                 data LONGBLOB
                              ) ENGINE=InnoDB DEFAULT CHARSET=latin1" ))
    {
	$@ = "could not create reordstore table : ". $dbh->errstr;
	return undef;
    }

    # ------------ create locks table ------------
    unless (defined $dbh->do( "CREATE TABLE IF NOT EXISTS locks (
                                 lockname varchar(256),
                                 unique index( lockname )
                              ) ENGINE=InnoDB DEFAULT CHARSET=latin1" ))
        #                              ) ENGINE=MyISAM DEFAULT CHARSET=latin1" ))
    {
	$@ = "could not create reordstore table : ". $dbh->errstr;
	return undef;
    }

    
    my $store =	bless {
        options      => {%options},
        
        dbh          => $dbh,
        
        times        => $dbh->prepare( "SELECT created,updated FROM recordstore WHERE id=?" ),

    }, $cls;
    
    return $store;
} #open_store

=head2 close_store()



=cut

sub close_recordstore {
    #noop
}

=head2 checkerr()



=cut

sub checkerr {
    my $self = shift;
    if ( my $err = $self->{dbh}->errstr ) {
        warn $err;
    }
}

=head2 get_record_count()



=cut

sub get_record_count {
    my $self = shift;
    my $sth = $self->{dbh}->prepare( "SELECT COUNT(*) FROM recordstore" );
    $sth->execute;
    $self->checkerr;
    my( $count ) = $sth->fetchrow_array;
    return $count;
}

sub root_id {
    my $self = shift;
    if (! $self->fetch(1)) {
        $self->next_id;
    }
    return 1;
}

=head2 fetch($id)



=cut

sub fetch {
    my( $self, $id ) = @_;
    my $sth = $self->{dbh}->prepare( "SELECT created, updated, data FROM recordstore WHERE id=?" );
    my $rv = $sth->execute( $id );
    $self->checkerr;
    my( $created, $updated, $data ) = $sth->fetchrow_array;
#    print STDERR "$id <------ $data\n";
    return ($created, $updated, $data);
} #fetch

=head2 stow($data, $id)



=cut

sub stow {
    my( $self, $data, $id ) = @_;

    $id //= $self->next_id;

#    print STDERR "$id =======> $data\n";
    
    if ($id < 1 || int($id) != $id) {
        $@ = "error : id for stow must be a positive integer if given";
        warn $@;
        return undef;
    }
    my $sth = $self->{dbh}->prepare( "UPDATE recordstore SET data=?,updated=ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000) WHERE id=?" );
    my $rv = $sth->execute( $data, $id );
    $self->checkerr;
    if ($rv > 0) {
	return $id;
    }
    return undef;
} #stow

sub show_locks {
    my ($self,$tag) = @_;
    my $has = 0;
    for my $q ( "show engine innodb status", "show full processlist" ) {
        my $sth = $self->{dbh}->prepare( $q );
        $sth->execute;
        my $has = 0;
        while ( my (@arr) = $sth->fetchrow_array ) {
            my $line = join( " ", @arr );
            if ($line =~ /((\d+) +lock struct[^\n\r]*[\n\r]+[^\n]*)/s && $2 > 0) {
#                print STDERR "<<$tag>>$1\n";
                $has = 1;
            }
        }
    }
    unless( $has ) {
#        print STDERR "<<$tag>>NO LOCK STRUCTS\n";
    }
}

sub connection_id {
    my ($self,$tag) = @_;
    my $sth = $self->{dbh}->prepare( "SELECT CONNECTION_ID()" );
    $sth->execute;
    my ($id) = $sth->fetchrow_array;    
#    print STDERR "<<$tag>> session id $id\n";
}

=head2 next_id()



=cut

sub next_id {
    my $self = shift;
    my $sth = $self->{dbh}->prepare( "INSERT INTO recordstore (created,updated) VALUES (ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000),ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000))" );
    my $rv = $sth->execute();
    $self->checkerr;
    my $id = $sth->last_insert_id;
    return $id;
} #next_id

=head2 delete_record( $del_id )



=cut

sub delete_record {
    my( $self, $del_id ) = @_;
    my $sth = $self->{dbh}->prepare( "UPDATE recordstore SET data=NULL WHERE id=?" );
    my $rv = $sth->execute( $del_id );
    $self->checkerr;
    unless (defined $rv) {
        $@ = "delete_record error : ".$sth->{dbh}->errstr;
        return undef;
    }
    return $rv;
} #delete_record

=head2 lock( @locknames )



=cut

sub lock {
    my( $self, @locknames ) = @_;
    my $locks = $self->{locks} //= {};
    my %tolock = map { $_ => 1 } @locknames;
    if (grep { ! $tolock{$_} } keys %$locks) {
        $@ = 'lock cannot be called twice in a row';
        return undef;
    }
    
    # make sure the locknames have entries
    my $sth = $self->{dbh}->prepare( "INSERT IGNORE INTO locks (lockname) VALUES (?)" );
    for my $name (@locknames) {
        unless( defined $sth->execute( $name ) ) {
            $@ = "lock got error :". $self->{dbh}->errstr;
            return undef;
        }
        $locks->{$name} = 1;
    }

    # start transaction and lock rows
    unless (defined ($self->{dbh}->do( "START TRANSACTION" ) )) {
        $@ = "lock got error :". $self->{dbh}->errstr;
        return undef;
    }

    if (@locknames) {
        unless (defined $self->{dbh}->do( "SELECT * FROM locks WHERE lockname IN (".join(",",map { '?' } @locknames).") FOR UPDATE", undef, @locknames ) ) {
            $@ = "lock got error :". $self->{dbh}->errstr;
            return undef;
        }
    }
    return 1;
} #lock

=head2 unlock()



=cut

sub unlock {
    my $self = shift;
    $self->{locks} = {};
    unless (defined $self->{dbh}->do( "COMMIT" )) {
        $@ = "unlock got error :". $self->{dbh}->errstr;
        return undef;
    }
    return 1;
} #unlock

=head2 use_transaction()



=cut

sub use_transaction {
    my $self = shift;
    unless (defined $self->{dbh}->do( "START TRANSACTION" )) {
        $@ = "use_transaction got error :". $self->{dbh}->errstr;
        return undef;
    }
    return 1;
} #use_transaction

=head2 commit_transaction()



=cut

sub commit_transaction {
    my $self = shift;
    unless (defined $self->{dbh}->do( "COMMIT" )) {
        $@ = "commit_transaction got error :". $self->{dbh}->errstr;
        return undef;
    }
    return 1;
} #commit_transaction

=head2 rollback_transaction()



=cut

sub rollback_transaction {
    my $self = shift;
    unless (defined $self->{dbh}->do( "ROLLBACK" )) {
        $@ = "rollback_transaction got error :". $self->{dbh}->errstr;
        return undef;
    }
    return 1;
} #rollback_transaction

=head2 detect_version()



=cut

sub detect_version {

} #detect_version



"I became Iggy because I had a sadistic boss at a record store. I'd been in a band called the Iguanas. And when this boss wanted to embarrass and demean me, he'd say, 'Iggy, get me a coffee, light.' - Iggy Pop";


