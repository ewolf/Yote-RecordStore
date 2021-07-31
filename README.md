# Yote-RecordStore
store serialized binary records
  
## Synopsis

```
use Yote::RecordStore::File;

my $store = Yote::RecordStore::File->open_store( "/some/data/directory" );

my $first_id = $store->first_id;
$store->stow( "some text or bytes", $first_id );

my $next_id = $store->stow( "more stuff to stow" );

my $rec = $store->fetch( $next_id ); # "more stuff to stow"

my $count = $store->get_record_count; # 2

$store->delete_record( 2 );
```

## Purpose

Store binary or text records fast. There is only one index - the id index.
This works along with Yote::ObjectStore which builds a tree of container nodes
from a root index. Each recordstore entry corresponds to a container node, and 
each container node stores the recordstore indexes of other container nodes it
contains.

```
