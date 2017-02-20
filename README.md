This Impala User Defined Aggregate Function (UDA) uses a very simple fixed size hashset to perform a distinct count on a column.  Impala does not currently allow multiple count distincts as the algorithm it employs for distinct count, while very efficient, does not lend itself to multiple counts.  This UDA was designed so that Impala users could perform mul

Disclaimers:
1. This UDA will not work if you have null characters ("\0") in your strings, it uses that character as a delimiter.
2. You should not use this for counting to very large numbers (many millions). This UDA will not perform as well as Impala's built in distinct count which was built to scale but it will allow you to perform multiple counts on moderately size cardinalities reasonably well.
3. For optimal performance you will need to ensure that the cardinatily (distinct items) in your column does not exceed the size of the hashset (default is 300k here, the bigger you make it the more memory it will use, so fit accordingly).
4. Currently at a minimum it will use at least the size of the hashset (300k default) * 8 bytes of space, in addition to the size of your data (not designed to be efficient), so an empty set will use at least 2.4MB per node.



To build: 

1. Install the impala udf development package: <http://archive.cloudera.com/cdh5/>
2. cmake .
3. make

The samples will get built to "build" directory, there is a test executable which runs some very basic tests, and an .so which you can use to install the function on the Impala cluster.


