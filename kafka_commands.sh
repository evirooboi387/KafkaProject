#!/usr/bin/env bash
# Reference commands: run each section manually, not this entire file.
# Run inside the Docker container as takeo.

cd ~/installAll/kafka_install

# Existing cluster verification
jps
kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Q1: Create and describe topic (only create if not already present)
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic applog --partitions 4 --replication-factor 3 --config min.insync.replicas=2
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic applog

# Q1: Run producer from project directory
(cd ~/pycharmprojects/KafkaProject && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 producer.py)
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic applog --from-beginning --property print.key=true --property print.partition=true --property key.separator=' : ' --max-messages 4

# Q2: Run streaming consumer; stop with Ctrl+C after viewing results
(cd ~/pycharmprojects/KafkaProject && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 consumer.py)

# Q3: Run this command in four separate Docker terminals
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic applog --group logConsumerGroup --from-beginning --property print.key=true --property print.partition=true --property key.separator=' : '
# In a separate verification terminal
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group logConsumerGroup
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group logConsumerGroup --members --verbose
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group logConsumerGroup --state
# Stop all four consumers with Ctrl+C, then verify Empty state before deletion
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group logConsumerGroup --state
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group logConsumerGroup
kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Q4: Create topic and publish all lines in a single execution
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic applog1 --partitions 4 --replication-factor 3 --config min.insync.replicas=2
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic applog1
kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic applog1 --property parse.key=true --property key.separator=':' --producer-property acks=all --producer-property enable.idempotence=true < ~/pycharmprojects/KafkaProject/application.log
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic applog1 --from-beginning --property print.key=true --property print.partition=true --property key.separator=' : ' --max-messages 4
