# Mikkoo

A [PgQ](https://wiki.postgresql.org/wiki/SkyTools#PgQ) to
[RabbitMQ](https://www.rabbitmq.com) relay. Mikkoo is a PgQ consumer that
publishes to RabbitMQ. In addition, it includes a built-in auditing system
that can be used to confirm that all PgQ events are received by RabbitMQ.

Mikkoo is named for the rabbit in the "Clever Rabbit and the Elephant" fable.

## Installation

Mikkoo is available on the [Python Package Index](https://pypi.python.org/pypi/mikkoo)
and can be installed via `pip`:

```bash
pip install mikkoo
```

Once you've setup [Skytools](https://wiki.postgresql.org/wiki/SkyTools) you
may want to install the optional included utility functions in
[mikkoo.sql](https://github.com/gmr/mikkoo/blob/main/mikkoo.sql) to make
usage easier.

You can do this with a combination of `curl` and `psql`:

```bash
curl -L https://github.com/gmr/mikkoo/blob/main/mikkoo.sql | psql
```

This will install multiple stored procedures and an audit table in a mikkoo
schema. Take a look at the DDL to get a good idea of what each function is
and how it can be used.

## Running Mikkoo

After creating a configuration file for Mikkoo, simply run the mikkoo
application providing the path to the configuration file:

```bash
mikkoo -c mikkoo.yml
```

The application will attempt to daemonize unless you use the `-f` foreground
CLI switch.

```bash
$ mikkoo -h
usage: mikkoo [-h] [-c CONFIG] [-f]

Mikkoo is a PgQ to RabbitMQ Relay

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to the configuration file
  -f, --foreground      Run the application interactively
```

## PgQ Event to AMQP Mapping

When inserting events into a PgQ queue, the `pgq.insert_event/7` function
should be used with the following field mappings:

| PgQ Event   | AMQP                  |
|-------------|-----------------------|
| `ev_type`   | Routing Key           |
| `ev_data`   | Message body          |
| `ev_extra1` | Exchange              |
| `ev_extra2` | Content-Type Property |
| `ev_extra3` | AMQP Properties [^1]  |
| `ev_extra4` | Headers [^2]          |
| `ev_time`   | Headers.timestamp     |
| `ev_txid`   | Headers.txid          |

[^1]: AMQP properties should be set as a JSON blob. Values set in the
    `ev_extra3` field will overwrite the automatically created properties
    `app_id`, `content_type`, `correlation_id`, `headers`, and `timestamp`.

[^2]: If `ev_extra4` is specified and is a JSON key/value dictionary, it will
    be assigned to the `headers` AMQP property.

### AMQP Message Properties

The following table defines the available fields that can be set in a JSON
blob in the `ev_extra3` field when inserting an event.

| Property           | PgSQL Type |
|--------------------|------------|
| `app_id`           | text       |
| `content_encoding` | text       |
| `content_type`     | text       |
| `correlation_id`   | text       |
| `delivery_mode`    | int2       |
| `expiration`       | text       |
| `message_id`       | text       |
| `headers`          | text/json  |
| `timestamp`        | int4       |
| `type`             | text       |
| `priority`         | int4       |
| `user_id`          | text       |

Values assigned in the JSON blob provided to `ev_extra3` take precedence over
the automatically assigned `app_id`, `content_type`, `correlation_id`,
`headers`, and `timestamp` values created by Mikkoo at processing time.

As of 1.0, Mikkoo will automatically add four AMQP headers property values.
These values will not overwrite any values with the same name specified in
`ev_extra4`. The `sequence` value is a dynamically generated ID that attempts
to provide fuzzy distributed ordering information. The `timestamp` value is
the ISO-8601 representation of the `ev_time` field, which is created when an
event is added to PgQ. The `txid` value is the `ev_txid` value, the PgQ
transaction ID for the event. These values are added to help provide some
level of deterministic ordering. The `origin` value is the hostname of the
server that Mikkoo is running on.

### Event Insertion Example

The following example inserts a JSON blob message body of `{"foo": "bar"}`
that will be published to the `postgres` exchange in RabbitMQ using the
`test.routing-key` routing key. The content type is specified in `ev_extra2`
and the AMQP `type` message property is specified in `ev_extra3`.

```sql
SELECT pgq.insert_event(
    'test',
    'test.routing-key',
    '{"foo": "bar"}',
    'postgres',
    'application/json',
    '{"type": "example"}',
    ''
);
```

When this message is received by RabbitMQ it will have a message body of:

```json
{"foo": "bar"}
```

And it will have message properties similar to the following:

| Property         | Example Value                                                     |
|------------------|-------------------------------------------------------------------|
| `app_id`         | `mikkoo`                                                          |
| `content_type`   | `application/json`                                                |
| `correlation_id` | `0ad6b212-4c84-4eb0-8782-9a44bdfe949f`                            |
| `headers`        | `origin=mikkoo.domain.com`, `sequence=4539586784185129828`, `timestamp=2017-02-23 06:17:14.471682-00`, `txid=41356335556` |
| `timestamp`      | `1449600290`                                                      |
| `type`           | `example`                                                         |
