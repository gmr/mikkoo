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
| ``ev_extra3`` | AMQP Properties [1]_  |
+---------------+-----------------------+
| ``ev_extra4`` | Headers [2]_          |
+---------------+-----------------------+

.. [1] AMQP properties should be set as a JSON blob. Values set in the ``ev_extra3``
       field will overwrite the automatically created properties ``app_id``,
       ``content_type``, ``correlation_id``, ``headers``, and ``timestamp``.
.. [2] If ``ev_extra4`` is specified and is a JSON key/value dictionary, it will
       be assigned to the ``headers`` AMQP property.

AMQP Message Properties
^^^^^^^^^^^^^^^^^^^^^^^
The following table defines the available fields that can be set in a JSON blob
in the ``ev_extra3`` field when inserting an event.

+----------------------+----------------+
| Property             | PgSQL Type     |
+======================+================+
| ``app_id``           | text           |
+----------------------+----------------+
| ``content_encoding`` | text           |
+----------------------+----------------+
| ``content_type``     | text           |
+----------------------+----------------+
| ``correlation_id``   | text           |
+----------------------+----------------+
| ``delivery_mode``    | int2           |
+----------------------+----------------+
| ``expiration``       | text           |
+----------------------+----------------+
| ``message_id``       | text           |
+----------------------+----------------+
| ``headers``          | text/json [3]_ |
+----------------------+----------------+
| ``timestamp``        | int4           |
+----------------------+----------------+
| ``type``             | text           |
+----------------------+----------------+
| ``priority``         | int4           |
+----------------------+----------------+
| ``user_id``          | text           |
+----------------------+----------------+

.. [3] ``headers`` should be sent to a key/value JSON blob if specified

Values assigned in the JSON blob provided to ``ev_extra3`` take precedence over
the automatically assigned ``app_id``, ``content_type``, ``correlation_id``,
``headers``, and ``timestamp`` values created by Mikkoo at processing time.

Event Insertion Example
-----------------------

The following example inserts a JSON blob message body of ``{"foo": "bar"}`` that
will be published to the ``postgres`` exchange in RabbitMQ using the ``test.routing-key``
routing key. The content type is specified in ``ev_extra2`` and the AMQP ``type``
message property is specified in ``ev_extra3``.

.. code:: sql

    # SELECT pgq.insert_event('test', 'test.routing-key', '{"foo": "bar"}', 'postgres', 'application/json', '{"type": "example"}', '');
     insert_event
    --------------
                4
    (1 row)

When this message is received by RabbitMQ it will have a message body of:

.. code:: json

    {"foo": "bar"}

And it will have message properties similar to the following:

+----------------------+------------------------------------------+
| Property             | Example Value                            |
+======================+==========================================+
| ``app_id``           | ``mikkoo``                               |
+----------------------+------------------------------------------+
| ``content_type``     | ``application/json``                     |
+----------------------+------------------------------------------+
| ``correlation_id``   | ``0ad6b212-4c84-4eb0-8782-9a44bdfe949f`` |
+----------------------+------------------------------------------+
| ``timestamp``        | ``1449600290``                           |
+----------------------+------------------------------------------+
| ``type``             | ``example``                              |
+----------------------+------------------------------------------+

Configuration
-------------
The Mikkoo configuration file uses `YAML <http://yaml.org>`_ for markup and allows
for one or more PgQ queue to be processed.

Queues are configured by name under the ``Application/workers`` stanza. The
following example configures two workers for the processing of a queue named
``invoices``. Each worker process connects to a local PostgreSQL and RabbitMQ
instance using default credentials.

.. code:: yaml

    Application:
      workers:
         invoices:
           postgres_url: postgresql://localhost:5432/postgres
           rabbitmq_url: amqp://localhost:5672/%2f
           processes: 2
           confirm: False

Queue Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following table details the configuration options available per queue:

+--------------------+---------------------------------------------------------------------+
| Key                | Description                                                         |
+====================+=====================================================================+
| ``postgresql_url`` | The url for connecting to PostgreSQL                                |
+--------------------+---------------------------------------------------------------------+
| ``rabbitmq_url``   | The AMQP url for connecting to RabbitMQ                             |
+--------------------+---------------------------------------------------------------------+
| ``processes``      | The number of worker processes to run for the queue. Default: ``1`` |
+--------------------+---------------------------------------------------------------------+
| ``confirm``        | Enable/Disable RabbitMQ Publisher Confirmations. Default: ``False`` |
+--------------------+---------------------------------------------------------------------+

Example Configuration
^^^^^^^^^^^^^^^^^^^^^

The following is an example of a full configuration file:

.. code:: yaml

    Application:

      poll_interval: 10

      statsd:
        enabled: true
        host: localhost
        port: 8125

      workers:
        test:
          postgres_url: postgresql://localhost:5432/postgres
          rabbitmq_url: amqp://localhost:5672/%2f
          processes: 1
          confirm: False

    Daemon:
      user: mikkoo
      pidfile: /var/run/mikkoo

    Logging:
      version: 1
      formatters: []
      verbose:
        format: '%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -10s %(funcName) -20s: %(message)s'
        datefmt: '%Y-%m-%d %H:%M:%S'
      handlers:
        console:
          class: logging.StreamHandler
          formatter: verbose
          debug_only: True
      loggers:
        helper:
          handlers: [console]
          level: INFO
          propagate: true
        mikkoo:
          handlers: [console]
          level: INFO
          propagate: true
        mikkoo.worker:
          handlers: [console]
          level: DEBUG
          propagate: true
        pika:
          handlers: [console]
          level: ERROR
          propagate: true
        queries:
          handlers: [console]
          level: ERROR
          propagate: true
        tornado:
          handlers: [console]
          level: ERROR
          propagate: true
      root:
        handlers: [console]
        level: CRITICAL
        propagate: true
      disable_existing_loggers: true
      incremental: false

