# golang-kafka

### Requirements

[Install and run kafka server](https://kafka.apache.org/quickstart)

Create is-even-request and is-event-reply topics

```shell
bin/kafka-topics.sh --create --topic is-even-request --bootstrap-server localhost:9092
```

```shell
bin/kafka-topics.sh --create --topic is-even-reply --bootstrap-server localhost:9092
```

### Kafka util scripts

list topics

```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```