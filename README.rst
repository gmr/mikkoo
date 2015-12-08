Mikkoo
======
A `PgQ <https://wiki.postgresql.org/wiki/SkyTools#PgQ>`_ to
`RabbitMQ <https://www.rabbitmq.com>`_ relay.


PgQ Setup
---------

1. Install ``pgq`` into your database and create the queue:

    .. code:: sql

        gavinr=# CREATE EXTENSION pgq;
        CREATE EXTENSION
        gavinr=# SELECT pgq.create_queue('test');

2. Ensure that `pgqd <http://skytools.projects.pgfoundry.org/skytools-3.0/doc/pgqd.html>`_
   is running.

PgQ Event to AMQP Mapping
-------------------------
When inserting events into a PgQ queue, the ``pgq.insert_event/7`` function
should be used with the following field mappings:

+---------------+-----------------------+
| PgQ Event     | AMQP                  |
+===============+=======================+
| ``ev_type``   | Routing Key           |
+---------------+-----------------------+
| ``ev_data``   | Message body          |
+---------------+-----------------------+
| ``ev_extra1`` | Exchange              |
+---------------+-----------------------+
| ``ev_extra2`` | Content-Type Property |
+---------------+-----------------------+
| ``ev_extra3`` | AMQP Properties[1]    |
+---------------+-----------------------+
| ``ev_extra4`` | Headers[2]            |
+---------------+-----------------------+

