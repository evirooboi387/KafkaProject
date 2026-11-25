# Kafka Application Log Processing

This project uses Apache Kafka and PySpark to publish and consume application logs, count words in each message, and manage consumer groups.

## Environment
- Kafka 3.2.0, ZooKeeper, Spark 3.3.2, Scala 2.12.15, Java 8
- Docker, PyCharm, Python 3.10
- Four brokers on ports 9092–9095, ZooKeeper on 2181

## Project tasks
- Q1: Create `applog` with four partitions, replication factor 3 and minimum ISR 2. Publish logs using PySpark with LogLevel as key and LogMessage as value.
- Q2: Consume `applog` using PySpark Structured Streaming, count words per message and display results.
- Q3: Start four console consumers in `logConsumerGroup`, verify one partition per consumer and zero lag, then stop the consumers and delete the group.
- Q4: Publish the entire log file to `applog1` using a console producer with colon-based key/value parsing, acknowledgements and idempotence.

## Run
Start ZooKeeper and the four brokers using the existing configuration. See `kafka_commands.sh` for commands to run individually; do not execute the entire file as a single script.

From the project directory inside Docker:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 consumer.py
```

For a portable producer, set `file_path = Path(__file__).resolve().with_name("application.log").as_uri()` after importing `Path` from `pathlib`. The consumer stores its checkpoint in the local filesystem (the original run used `file:///home/takeo/bootcamp/kafka-log-project/checkpoints`). Stop the streaming consumer with Ctrl+C after viewing the output. Re-running the producer publishes additional records.

## Verified results
| LogLevel | LogMessage | WordCount |
|---|---|---:|
| ERROR | Invalid jdbc url | 3 |
| INFO | Connecting to mysql | 3 |
| WARN | Using older mysql connector | 4 |
| DEBUG | Successfully connected to mysql | 4 |

Both topics had four partitions and three in-sync replicas per partition. The consumer group was verified as Stable with four members and zero lag. Group deletion was reported successful by the project author. The console consumer verified four messages in `applog1`.

## Notes
Kafka hashes keys to select partitions; distinct keys can share a partition. The console producer preserves surrounding whitespace while the PySpark producer removes it. All brokers run inside one Docker container, so replication provides broker-level rather than host-level fault tolerance. Screenshots belong in `screenshots/` and the report in `docs/`.
