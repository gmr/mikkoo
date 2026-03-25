# Mikkoo

A [PgQ](https://wiki.postgresql.org/wiki/SkyTools#PgQ) to
[RabbitMQ](https://www.rabbitmq.com) relay. Mikkoo is a PgQ consumer that
publishes to RabbitMQ. In addition, it includes a built-in auditing system
that can be used to confirm that all PgQ events are received by RabbitMQ.

Mikkoo is named for the rabbit in the "Clever Rabbit and the Elephant" fable.

![Version](https://img.shields.io/pypi/v/mikkoo.svg?)
![Downloads](https://img.shields.io/pypi/dm/mikkoo.svg?)
![Status](https://github.com/gmr/mikkoo/actions/workflows/testing.yaml/badge.svg?)

## Installation

```bash
pip install mikkoo
```

## Documentation

Documentation is available at [gmr.github.io/mikkoo](https://gmr.github.io/mikkoo/).

## Running

```bash
mikkoo -c mikkoo.yml
```
