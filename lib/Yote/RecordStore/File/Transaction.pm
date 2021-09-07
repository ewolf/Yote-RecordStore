package Yote::RecordStore::File::Transaction;

# TODO - verify perldoc
#      - shed/keep YAML?
#      - remove die, replace with return values
#      - enable testing

use strict;
use warnings;
no warnings 'numeric';
no warnings 'uninitialized';

use File::Path qw(make_path remove_tree);

use vars qw($VERSION);
$VERSION = '0.01';

use constant {
    RS_ACTIVE    => 1,
    RS_DEAD      => 2,
    RS_IN_TRANSACTION => 3,

    TR_ACTIVE         => 1,
    TR_IN_COMMIT      => 2,
    TR_IN_ROLLBACK    => 3,
    TR_COMPLETE       => 4,
};

#######################################################################
# Transactions use a stack silo to record what happens                #
# to entries affected by the transaction.                             #
#                                                                     #
# When an entry is deleted in a transaction, the stack silo           #
# marks that entry for deletion.                                      #
#                                                                     #
# When entry data is stowed in a transaction, the transaction         #
# stores the data in the store. It marks this location as well        #
# as the original location of the entry data (if any).                #
#                                                                     #
# The stack silo keeps only the last record for an entry, so          #
# if an entry is stowed, then deleted, then stowed again, only        #
# the most recent action is recorded. The stack silo contains the     #
# id, the state, the original silo id (if there is one ),             #
# the original id in the silo (if there is one ), the silo id that   #
# the transaction stored data in, the id in the silo that the        #
# the transaction stored data in                                      #
#                                                                     #
# When fetch is used from a transaction, the stack silo is checked    #
# to see if there was any action on the fetched id. If so, it returns #
# the transactional value.                                            #
#######################################################################


sub create {
    my( $cls, $store ) = @_;

    my $trans_id = $store->[$store->TRANSACTION_INDEX_SILO]->push( [TR_ACTIVE, 0] );
    return bless {
        state        => TR_ACTIVE,
        store        => $store,
        trans_id     => $trans_id,
        updates      => {},
    }, $cls;

} #create

sub _err {
    my ($method,$txt) = @_;
    $@ = __PACKAGE__."::$method $txt";
}

sub open {
    my( $cls, $store, $trans_id ) = @_;

    my ($trans_state, $trans_obj_id) = @{$store->[$store->TRANSACTION_INDEX_SILO]->get_record( $trans_id ) || []};
;

    unless ($trans_obj_id) {
        _err( 'open', 'no transaction object was recorded' );
        return undef;
    }

    my $update_data = $store->_fetch( $trans_obj_id );
    my (@update_all) = unpack "I*", $update_data;
    my $updates = {};
    while (@update_all) {
        my ($status,$id,$orig_silo_id,$orig_id_in_silo,$trans_silo_id,$trans_id_in_silo) =  splice @update_all, 0, 6;
        $updates->{$id} = [$status,$id,$orig_silo_id,$orig_id_in_silo,$trans_silo_id,$trans_id_in_silo];
    }
    

    return bless {
        state        => $trans_state,
        store        => $store,
        trans_obj_id => $trans_obj_id,
        trans_id     => $trans_id,
        updates      => $updates,
    }, $cls;
}


sub commit {
    my $self = shift;

    my $store = $self->{store};

    my $updates = $self->{updates};
    my @update_ids = keys %$updates;

    my $trans_obj_template = "IIIIII" x @update_ids; 

    my $trans_obj_id = $store->_stow( pack( $trans_obj_template, map { @$_ } values %$updates) );

    # mark transaction as in commit
    unless ($store->transaction_silo->put_record( $self->{trans_id}, [TR_IN_COMMIT, $trans_obj_id] )) {
        return undef;
    }

    $self->{state} = TR_IN_COMMIT;

    my $store_index = $store->index_silo;
    my $store_silos = $store->silos;
    #
    # first update the index
    # 
    for my $id (@update_ids) {
        my( $action, $rec_id, $orig_silo_id, $orig_id_in_silo, $trans_silo_id, $trans_id_in_silo ) = @{$updates->{$id}};
        if( $action == RS_ACTIVE ) {
            # create or update
            unless ($store_index->put_record( $rec_id, [$trans_silo_id,$trans_id_in_silo], 'IL' )) {
                return undef;
            }
        }
        else {
            # remove
            unless ($store_index->put_record( $rec_id, [0,0], 'IL' )) {
                return undef;
            }
        }
    }


    #
    # now update the data silo records to make these active
    # 
    for my $id (@update_ids) {
        my( $action, $rec_id, $orig_silo_id, $orig_id_in_silo, $trans_silo_id, $trans_id_in_silo ) = @{$updates->{$id}};
        if( $action == RS_ACTIVE ) {
            # create or update
            unless ($store_silos->[$trans_silo_id]->put_record( $trans_id_in_silo, [ RS_ACTIVE ], 'I' )) {
                return undef;
            }
        }
        else {
            # remove
            if ($orig_silo_id) {
                # if it existed, mark it as dead in the data silo
                unless ($store->silos->[$orig_silo_id]->put_record( $orig_id_in_silo, [RS_DEAD], 'I' )) {
                    return undef;
                }
            }
        }
    }

    #
    # Now mark the transaction as complete
    #
    unless ($store->transaction_silo->put_record( $self->{trans_id}, [TR_COMPLETE], 'I' )) {
        return undef;
    }

    # mark the trans object as dead
    my( $trans_obj_silo_id, $trans_obj_id_in_silo ) = @{$store->[$store->INDEX_SILO]->get_record($trans_obj_id)};
    $store->silos->[$trans_obj_silo_id]->put_record( $trans_obj_id_in_silo, [RS_DEAD], 'I' );

    # vacate the trans_obj_id
    $store->_vacate( $trans_obj_silo_id, $trans_obj_id_in_silo );

    # this is sort of linting. The transaction is complete, but this cleans up any records marked deleted.

    my @update_blocks =
               sort { $b->[3] <=> $a->[3] }
               map { $updates->{$_} }
               @update_ids;

    for my $block (@update_blocks) {
        my( $action, $rec_id, $orig_silo_id, $orig_id_in_silo ) = @$block;
        if( $orig_silo_id ) {
           $store->_vacate( $orig_silo_id, $orig_id_in_silo );
        }
    }
    return 1;
} #commit

sub rollback {
    my $self = shift;
    my $store = $self->{store};
    my $index = $store->index_silo;

    $store->transaction_silo->put_record( $self->{id}, [TR_IN_ROLLBACK], 'I' );
    $self->{state} = TR_IN_ROLLBACK;

    # [RS_ACTIVE, $id, $orig_silo_id, $orig_id_in_silo, $trans_silo_id, $trans_id_in_silo];
    # [RS_DEAD  , $id, $orig_silo_id, $orig_id_in_silo, 0, 0];
    
    my $updates = $self->{updates};
    my @update_ids = keys %$updates;

    # must be sorted so that the ones on the ends of the silos get removed first
    # otherwise, an earlier can try to swap out with a later one and the later one wont move
    my @sorted_update_blocks = 
        sort { $b->[5] <=> $a->[5] }
        map { $updates->{$_} } 
        keys %$updates;

    for my $block (@sorted_update_blocks) {
        my( $action, $rec_id, $orig_silo_id, $id_in_orig_silo, $trans_silo_id, $trans_id_in_silo ) = @$block;

#    for my $id (@update_ids) {
#        print STDERR "ROLLBACK $id\n";
#        my( $action, $rec_id, $orig_silo_id, $id_in_orig_silo, $trans_silo_id, $trans_id_in_silo ) = @{$updates->{$id}};
        my( $reported_silo_id, $id_reported_silo ) = @{$index->get_record( $rec_id )};
        if( $reported_silo_id != $orig_silo_id || $id_reported_silo != $id_in_orig_silo ) {
            $index->put_record( $rec_id, [$orig_silo_id,$id_in_orig_silo], "IL" );            
        }
        if( $trans_silo_id ) {
            $store->_vacate( $trans_silo_id, $trans_id_in_silo );
        }
    }

    $store->transaction_silo->put_record( $self->{id}, [TR_COMPLETE], 'I' );
    $self->{state} = TR_COMPLETE;
    return 1;
} #rollback

sub fetch {
    my( $self, $id ) = @_;

    my $store = $self->{store};

    my $updates = $self->{updates};
    if( my $rec = $updates->{$id} ) {
        my( $action, $rec_id, $a, $b, $trans_silo_id, $trans_id_in_silo ) = @$rec;    
        if( $action == RS_ACTIVE ) {
            my $ret = $store->silos->[$trans_silo_id]->get_record( $trans_id_in_silo );
            return substr( $ret->[3], 0, $ret->[2] );
        }
        return undef;
    }
    return $store->_fetch( $id );
} #fetch

sub fix {
    my $self = shift;
    my $trans_state = $self->{state};
    if ($trans_state != TR_COMPLETE) {
        $self->rollback;
    }
}

#####################################################################################################################
# given data and id,                                                                                                #
#                                                                                                                   #
#  uses the data size to find the appropriate silo id (new-silo-id) to store the data.                              #
#                                                                                                                   #
#  looks up the original silo-id, index-in-silo from the stores index,                                              #
#    which may or not exist                                                                                         #
#                                                                                                                   #
#  push the new data value into the store silo with the new-silo-id from above                                      #
#                                                                                                                   #
#  sees if the id already has an entry in the stack silo                                                            #
#     - if yes, it updates it to include STOW,id,new_silo_id,new_id_in_silo,$original-silo-id,original-id-in-silo   #
#     - if no, pushes on to it to STOW,id,new_silo_id,new_id_in_silo,$original-silo-id,original-id-in-silo          #
#####################################################################################################################
sub stow {
    my ($self, $data, $id ) = @_;

    my $store = $self->{store};
    if( $id == 0 ) {
        $id = $store->next_id;
    }

    my $data_write_size = do { use bytes; length( $data ) };
    my $trans_silo_id = $store->_silo_id_for_size( $data_write_size );

    my $trans_silo = $store->silos->[$trans_silo_id];
    my( $orig_silo_id, $orig_id_in_silo ) = @{$store->index_silo->get_record($id)};
    my $trans_id_in_silo = $trans_silo->push( [RS_IN_TRANSACTION, $id, $data_write_size, $data] );

    my $update = [RS_ACTIVE,$id,$orig_silo_id,$orig_id_in_silo,$trans_silo_id,$trans_id_in_silo];
    $self->{updates}{$id} = $update;

    return $id;
} #stow

sub delete_record {
    my( $self, $id ) = @_;
    
    delete $self->{updates}{$id};

    my( $orig_silo_id, $orig_id_in_silo ) = @{$self->{store}->index_silo->get_record($id)};
    unless ($orig_silo_id) {
        return undef;
    }
    
    my $update = [RS_DEAD,$id,$orig_silo_id,$orig_id_in_silo,0,0];
    $self->{updates}{$id} = $update;

    return $id;
} #delete_record

"I think there comes a time when you start dropping expectations. Because the world doesn't owe you anything, and you don't owe the world anything in return. Things, feelings, are a very simple transaction. If you get it, be grateful. If you don't, be alright with it. - Fawad Khan";

__END__

=head1 NAME

 Yote::RecordStore::File::Transaction - Transaction support for Yote::RecordStore::File

=head1 DESCRIPTION

This is used by Yote::RecordStore::File and is not meant for use outside of it.

=head1 METHODS

=head2 create

=head2 commit

=head2 rollback

=head2 fetch( id )

Returns the record associated with the ID. If the ID has no
record associated with it, undef is returned.

=head2 stow( data, optionalID )

This saves the text or byte data to the record store.
If an id is passed in, this saves the data to the record
for that id, overwriting what was there.
If an id is not passed in, it creates a new record store.

Returns the id of the record written to.

=head2 delete_record( id )

Removes the entry with the given id from the store, freeing up its space.
It does not reuse the id.


=head1 AUTHOR
       Eric Wolf        coyocanid@gmail.com

=head1 COPYRIGHT AND LICENSE

       Copyright (c) 2015-2019 Eric Wolf. All rights reserved.
       This program is free software; you can redistribute it
       and/or modify it under the same terms as Perl itself.

=head1 VERSION
       Version 0.01  (Oct, 2019))

=cut
