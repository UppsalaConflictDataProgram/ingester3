### Important!

You need these triggers in order to make the whole ingester system work.
What they do is this: 

- Identify when there is a modification in the `DDL` (a `DROP, ALTER, CREATE, CREATE AS`) on the server.
- Identify whether the `DDL` change took place in the `prod` stamp.
- If above, changes the timestamp in the `metadata` to `NOW()`.
- This timestamp in metadata determines the caching that `ingestion` clients use. If the metadata shows a DDL change, it regenerates the cache.

**WITHOUT THESE TRIGGERS THE INGESTER DOES NOT WORK WELL AT ALL**

Note that there are two triggers, one for `DROP`s and one for `CREATE` or `ALTER`.