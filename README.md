This Impala User Defined Aggregate Function (UDA) uses a very simple fixed size 'hashset' (it's really just an array) to perform a distinct count on a column.  Impala does not currently allow multiple count distincts as the algorithm it employs for distinct count, while very efficient, does not lend itself to multiple counts.  This UDA was designed so that Impala users could perform multiple distinct counts at the same time, though not as efficiently or as flexibly as the built-in.  It will work best for distinct counts (cardinality) near the bucket size (300k default here), and it likely wont perform too well with very high counts as the final aggregation would be doing a large amount of work.


#### Disclaimers:

1. This UDA will not work if you have null characters ("\0") in your strings, it uses that character as a delimiter.
2. You should not use this for counting to very large numbers (many millions, billions). This UDA will not perform as well as Impala's built in distinct count which was built to scale, however it will allow you to perform multiple counts on moderately size cardinalities reasonably well.
3. For optimal performance you will need to ensure that the cardinatily (distinct items) in your column does not exceed the size of the hashset (default is 300k here, the bigger you make it the more memory it will use, so fit accordingly).
4. Currently at a minimum it will use at least the size of the hashset (300k default) * 8 bytes of space, in addition to the size of your data (not designed to be efficient), so an empty set will use at least 2.4MB per node.


#### To build: 

1. Install the impala udf development package: <http://archive.cloudera.com/cdh5/>
2. cmake .
3. make

The UDA will get built to "build" directory, there is a test executable which runs some very basic tests locally, and an .so which you can use to install the function on the Impala cluster.

To install on cluster:

1. Place .so file in HDFS
2. Run the following in Impala:

> CREATE AGGREGATE FUNCTION count300k(string) RETURNS STRING
> LOCATION '/path/to/libhashsetcount.so'
> init_fn='DistHashSetInit'
> update_fn='DistHashSetUpdate'
> merge_fn='DistHashSetMerge'
> finalize_fn='DistHashSetFinalize';


#### More about how it works:
* First the update function will go through the column inserting values into our 'hashset' and appending where there are collisions (delimited by /0)
* When we serialize the hashset, we turn it into a delimited list (using \0) that is sorted by position in the hashset.
* This uses a string as the intermediate type as a sort of byte array or buffer.
* The first byte of the intermediate type is a 'magic byte' that indicates whether we're passing around a _H_ashset struct or a _D_elimited string that is ordered by the hash bucket value.
* Since the delimeted list is ordered by hash bucket, we can do a variation on a sort-merge join between different lists in the merge function.  This is relatively efficient so long as we don't have too many collisions, where there are collisions (same hash bucket) we have to compare each value to make sure there are no duplicates.
* Finally, we simply count the number of delimiters to know how many objects are in our final list.
