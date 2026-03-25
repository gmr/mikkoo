# PgQ Setup

## Installing PgQ

Install `pgq` into your database and create the queue:

```sql
CREATE EXTENSION pgq;

SELECT pgq.create_queue('test');
```

Ensure that [pgqd](http://skytools.projects.pgfoundry.org/skytools-3.0/doc/pgqd.html)
is running.

## Utility Functions

There is a convenience schema in the
[mikkoo.sql](https://github.com/gmr/mikkoo/blob/main/mikkoo.sql) file that
adds stored procedures for creating properly formatted mikkoo events in PgQ.
In addition, there are auditing functions that allow for the creation of an
audit-log of events that were sent to PgQ.
