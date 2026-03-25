# Configuration

The Mikkoo configuration file uses [YAML](http://yaml.org) for markup and
allows for one or more PgQ queues to be processed.

If you have a Sentry account, the `Application/sentry_dsn` setting will turn
on Sentry exception logging, if the
[sentry-sdk](https://pypi.python.org/pypi/sentry-sdk) client library is
installed.

Queues are configured by name under the `Application/workers` stanza. The
following example configures two workers for the processing of a queue named
`invoices`. Each worker process connects to a local PostgreSQL and RabbitMQ
instance using default credentials.

```yaml
Application:
  workers:
     invoices:
       postgres_url: postgresql://localhost:5432/postgres
       rabbitmq:
         host: localhost
         port: 5671
         vhost: /
         ssl_options:
           protocol: 2
       confirm: false
```

## Queue Configuration Options

The following table details the configuration options available per queue:

| Key              | Description                                                        |
|------------------|--------------------------------------------------------------------|
| `confirm`        | Enable/Disable RabbitMQ Publisher Confirmations. Default: `False`  |
| `consumer_name`  | Overwrite the default PgQ consumer name. Default: `mikkoo`         |
| `max_failures`   | Maximum failures before discarding an event. Default: `10`         |
| `postgres_url`   | The URL for connecting to PostgreSQL                               |
| `rabbitmq`       | Data structure for connection parameters to connect to RabbitMQ    |
| `retry_interval` | How long in seconds until retrying failed events. Default: `10`    |
| `unregister`     | Unregister a consumer with PgQ on shutdown. Default: `True`        |
| `wait_duration`  | How long to wait before checking the queue after the last empty result. Default: `10` |

### `rabbitmq` attributes

| Attribute            | Description                                              |
|----------------------|----------------------------------------------------------|
| `host`               | The hostname or ip address of the RabbitMQ server (str)  |
| `port`               | The port of the RabbitMQ server (int)                    |
| `vhost`              | The virtual host to connect to (str)                     |
| `username`           | The username to connect as (str)                         |
| `password`           | The password to use (str)                                |
| `ssl_options`        | Optional: the SSL options for the SSL connection socket  |
| `heartbeat_interval` | Optional: the AMQP heartbeat interval (int) default: 300 sec |

### `ssl_options` attributes

| Attribute   | Description                                                                         |
|-------------|-------------------------------------------------------------------------------------|
| `ca_certs`  | The file path to the concatenated list of CA certificates (str)                     |
| `ca_path`   | The directory path to the PEM formatted CA certificates (str)                       |
| `ca_data`   | The PEM encoded CA certificates (str)                                               |
| `protocol`  | The ssl `PROTOCOL_*` enum integer value. Default: `2` for `PROTOCOL_TLS` (int)      |
| `certfile`  | The file path to the PEM formatted certificate file (str)                           |
| `keyfile`   | The file path to the certificate private key (str)                                  |
| `password`  | The password for decrypting the `keyfile` private key (str)                         |
| `ciphers`   | The set of available ciphers in the OpenSSL cipher list format (str)                |

## Example Configuration

The following is an example of a full configuration file:

```yaml
Application:

  poll_interval: 10
  sentry_dsn: [YOUR SENTRY DSN]

  statsd:
    enabled: true
    host: localhost
    port: 8125

  workers:
    test:
      confirm: False
      consumer_name: my_consumer
      max_failures: 5
      postgres_url: postgresql://localhost:5432/postgres
      rabbitmq:
        host: localhost
        port: 5671
        username: guest
        password: guest
        ssl_options:
          protocol: 2
      retry_interval: 5
      unregister: False
      wait_duration: 5

Daemon:
  user: mikkoo
  pidfile: /var/run/mikkoo

Logging:
  version: 1
  formatters:
    verbose:
      format: '%(levelname) -10s %(asctime)s  %(process)-6d %(processName) -20s %(name) -18s: %(message)s'
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
    pika:
      handlers: [console]
      level: ERROR
      propagate: true
  root:
    handlers: [console]
    level: CRITICAL
    propagate: true
  disable_existing_loggers: true
  incremental: false
```
