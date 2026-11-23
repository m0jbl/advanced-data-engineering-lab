# Lab 02 · Kafka keys, groups, and lag

**Time:** 60 minutes. **Starting point:** [Lab 01 complete](lab-01-start-the-platform.md), with the core services running. Run commands from the lab repository root. Use your own Codespace or laptop stack so other producers do not change your measurements.

## Goal

Use payment records to explain partitioning by passenger, how one consumer group shares partitions, and what unprocessed records look like in Kafka. You do not need the Spark fraud scorer for this lab.

## Playbook

### 1. Follow passenger keys into partitions

Send synthetic payments for 20 seconds:

```bash
PYTHONPATH=src uv run python -m lab.producer --seconds 20
```

Read up to 15 records. The formatter shows the partition, offset, and key before the JSON value. Use `airport.transactions`, not `airport.fraud-alerts`.

**Codespace**

```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --from-beginning --max-messages 15 --timeout-ms 10000 --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

**Laptop (Docker)**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --from-beginning --max-messages 15 --timeout-ms 10000 --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

**Checkpoint:** record key, partition, and offset for three distinct passengers. Check that each key matches `passenger_id`. If you find a repeated key in the sample or in Kafka UI, compare its partitions. If not, report that you did not observe a repeat. Explain why ordering within a passenger's partition matters, and why this is not ordering across the entire topic.

### 2. Watch two members share one group

Open terminals A and B. In each terminal, run the same command to join `lab02-workers`. Keep both consumers running.

**Codespace**

```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --group lab02-workers --from-beginning --consumer-property enable.auto.commit=true --consumer-property auto.commit.interval.ms=1000 --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

**Laptop (Docker)**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --group lab02-workers --from-beginning --consumer-property enable.auto.commit=true --consumer-property auto.commit.interval.ms=1000 --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

In terminal C, describe the members and their assignments. Run it again if Kafka is still rebalancing.

**Codespace**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group lab02-workers --describe --members --verbose
```

**Laptop (Docker)**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group lab02-workers --describe --members --verbose
```

**Checkpoint:** two members together own all three partitions. No partition is assigned twice in this group. Save the output. Leave both consumers running for Step 3.

### 3. Show that two groups can read the same record

In terminal C, start a consumer in a separate group, `lab02-analytics`. Leave it running alongside the workers.

**Codespace**

```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --group lab02-analytics --from-beginning --consumer-property enable.auto.commit=false --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

**Laptop (Docker)**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic airport.transactions --group lab02-analytics --from-beginning --consumer-property enable.auto.commit=false --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

Once all three consumers have joined, open terminal D and send another burst using the producer command from Step 1. Compare one of the new records in C with the same record in A or B. Match both partition and offset; an offset alone only identifies a position within one partition.

**Checkpoint:** save one matching partition and offset from the outputs of `lab02-workers` and `lab02-analytics`. The groups read independently; the records are not duplicated in the topic. Stop the analytics consumer and leave both workers running.

### 4. Measure lag growing and draining

In terminal C, check the committed offsets for `lab02-workers`. Reuse this command for each measurement.

**Codespace**

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group lab02-workers --describe
```

**Laptop (Docker)**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group lab02-workers --describe
```

1. **Before:** wait until `CURRENT-OFFSET` is numeric and `LAG` is zero for all three partitions. Save this baseline.
2. **Behind:** stop every worker in `lab02-workers` with Ctrl+C. Send another 20-second burst with the producer command in Step 1. Describe the group again. Save rows where `LOG-END-OFFSET` rose but `CURRENT-OFFSET` did not.
3. **Caught up:** restart one worker with the command in Step 2. Wait for it to consume the burst and commit its offsets. Describe the group again until `LAG` is zero. Save this result and stop the consumer. Leave Kafka and the core stack running for Lab 03.

## Output evidence to submit

Submit one `lab-02-evidence.md` with your environment and the following actual results. If you could not observe a checkpoint, record what you saw instead.

| Evidence | What it must show |
|---|---|
| Key and partition sample | Three passenger keys, partitions, and offsets. Note what you observed for any repeated key. |
| Same-group assignment | `lab02-workers` with two members and assignments that cover all three partitions without overlap. |
| Independent groups | The same `airport.transactions` partition and offset in the worker and analytics outputs. |
| Lag measurements | `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG` for all three partitions at baseline, behind, and caught up. |

Explain in two or three sentences how a stopped consumer can increase the delay before a fraud decision reaches an operator.
