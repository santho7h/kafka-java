# Numaflow Kafka Sourcer/Sinker

## Overview

Numaflow Kafka Sourcer/Sinker is a [Numaflow](https://numaflow.numaproj.io/) user-defined source/sink connector for
Apache Kafka. It allows you to read/write data from/to a Kafka topic using Numaflow. Integration with Confluent Schema
Registry is also supported.

## Use Cases

### Read data from Kafka

Use Case 1: Read data from Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/source/avro/avro-source.md).

Use Case 2: Read data from Kafka with no schema or JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/source/non-avro/non-avro-source.md).

### Write data to Kafka

Use Case 3: Write data to Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/avro/avro-sink.md).

Use Case 4: Write data to Kafka with a JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/json/json-sink.md).

Use Case 5: Write data to Kafka with no schema. See an example [here](docs/sink/no-schema/no-schema-sink.md).

## FAQ

### How do I configure logging?

This application uses **SLF4J** with **Logback** for logging (via Lombok `@Slf4j`).

The application ships with a `logback.xml` that defaults to `INFO` level and supports runtime configuration via the `ROOT_LOG_LEVEL` environment variable.

#### How do I configure logging level?

Set the `ROOT_LOG_LEVEL` environment variable in your container spec:

```yaml
env:
  - name: ROOT_LOG_LEVEL
    value: "WARN"
```

Available levels: `TRACE`, `DEBUG`, `INFO` (default), `WARN`, `ERROR`, `OFF`

#### How do I enable structured logging (JSON)?

Add the `logstash-logback-encoder` dependency and configure a JSON encoder in your `logback.xml`. Refer to the [logstash-logback-encoder documentation](https://github.com/logfellow/logstash-logback-encoder) for dependency coordinates and encoder configuration.