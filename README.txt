cassandra-ehcache
=================

Combine Cassandra with Ehcache using triggers

An experiment with Cassandra 2.0, triggers, and Ehcache as an "external" cache.  A REST service is instantiated and runs in the Cassandra JVM.  The idea is that a client would interact with the data via the REST service, caching in ehcache on GET, but not on PUT.

The Cassandra trigger comes into the picture when a client other than the REST service updates Cassandra.  It will update the cache with the new column values.

Really nothing to report, but there are tests that run properly and demonstrate what could be done with this type of setup.
