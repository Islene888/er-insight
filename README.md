# ER-Insight — Multi-Cloud Data Ingestion Pipeline

A production-grade, fault-tolerant data ingestion pipeline for high-volume Electronic Records (ER) data. Ingests from **Google Cloud Pub/Sub** and **AWS SQS/SNS** in parallel, normalizes heterogeneous record types, and writes to **MongoDB** with exactly-once delivery guarantees.

## Key Stats

| Metric | Value |
|---|---|
| Daily record volume | 200K+ records/day |
| Monthly message throughput | 100M+ messages/month |
| Delivery guarantee | Exactly-once (idempotent consumers + DLQ) |
| Sources | GCP Pub/Sub + AWS SQS/SNS |
| Storage | MongoDB (sharded by `source_region`) |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    External Sources                      │
│   GCP Pub/Sub          AWS SQS/SNS        (future: Kafka)│
└──────┬────────────────────────┬───────────────────────── ┘
       │                        │
       ▼                        ▼
┌──────────────┐       ┌──────────────┐
│ PubSubConsumer│       │  SQSConsumer  │   (parallel threads)
└──────┬───────┘       └──────┬───────┘
       │   idempotency check  │
       └──────────┬───────────┘
                  ▼
         ┌────────────────┐
         │  BaseConsumer  │  dedup via processed_messages index
         │  + DLQ routing │  retry with exponential backoff
         └───────┬────────┘
                 │  batch upsert
                 ▼
         ┌────────────────┐      ┌──────────────────┐
         │   MongoWriter  │────► │  MongoDB          │
         │  (bulk_write)  │      │  er_records       │
         └────────────────┘      │  processed_msgs   │
                                 └──────────────────┘
                 │ metrics
                 ▼
         ┌────────────────┐
         │   Prometheus   │ ──► Grafana dashboard
         └────────────────┘
```

## Project Structure

```
er-insight/
├── src/
│   ├── consumers/
│   │   ├── base_consumer.py       # Exactly-once base (atomic lock + retry + Cloud Tasks DLQ)
│   │   ├── pubsub_consumer.py     # GCP Pub/Sub consumer
│   │   └── sqs_consumer.py        # AWS SQS/SNS consumer
│   ├── schema/
│   │   └── er_record.py           # MongoDB document schema (5 record types)
│   ├── storage/
│   │   └── mongo_writer.py        # Batched bulk_write with upsert
│   ├── metrics/
│   │   └── prometheus_metrics.py  # Counters, histograms, gauges (incl. replication lag)
│   ├── retry/
│   │   ├── cloud_tasks_queue.py   # GCP Cloud Tasks delayed retry (30s/2m/10m/1h schedule)
│   │   └── retry_handler.py       # FastAPI endpoint that receives Cloud Tasks callbacks
│   ├── replication/
│   │   └── region_replicator.py   # MongoDB change stream → multi-region upsert
│   └── pipeline/
│       └── ingestion_pipeline.py  # Orchestrator (parallel threads + replication)
├── tests/
│   ├── test_base_consumer.py
│   ├── test_schema.py
│   ├── test_mongo_writer.py
│   └── test_region_replicator.py
├── config/
│   └── config.yaml.example
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Exactly-Once Delivery

Instead of the common (broken) find-then-insert pattern, we use an **atomic insert as a distributed lock**:

```
insert_one({message_id, status: "processing"})   ← atomic claim
    ├── DuplicateKeyError → skip (another instance owns it)
    └── success → handle() → update status to "done"
                     └── failure → update to "failed" → Cloud Tasks retry
```

This eliminates the TOCTOU race condition under concurrent consumers.

## Cloud Tasks Retry Schedule

Failed messages (after 3 in-process retries) are enqueued to GCP Cloud Tasks with exponential delays rather than immediately dropped to DLQ:

| Attempt | Delay |
|---|---|
| 1 | 30 seconds |
| 2 | 2 minutes |
| 3 | 10 minutes |
| 4 | 1 hour |
| 5+ | Permanent DLQ |

Cloud Tasks calls back `POST /retry` on our FastAPI handler, which reprocesses from a clean context.

## Cross-Region Replication

`RegionReplicator` watches the primary MongoDB **change stream** and fans out to secondary instances in other regions:

```python
SECONDARY_REGIONS='{"us-west-2": "mongodb://...", "eu-west-1": "mongodb://..."}'
```

- Resume token persisted in MongoDB — restarts replay from last processed event, no data loss
- Idempotent upserts (`$setOnInsert`) — safe to replay
- Per-region replication error counter in Prometheus

## Tech Stack

- **Messaging**: Google Cloud Pub/Sub, AWS SQS/SNS
- **Storage**: MongoDB 7 (sharded collection, compound indexes)
- **Observability**: Prometheus metrics, structured logging
- **Reliability**: Idempotent consumers, dead-letter queues, exponential backoff, bulk upsert
- **Infra**: Docker Compose (local), deployable to GKE / ECS

## Quick Start

```bash
# 1. Clone and install
pip install -r requirements.txt

# 2. Configure
cp config/config.yaml.example config/config.yaml
# Edit config.yaml with your GCP project, AWS credentials, MongoDB URI

# 3. Run locally with Docker Compose (spins up MongoDB + pipeline)
docker-compose up

# 4. Run pipeline directly
export MONGO_URI=mongodb://localhost:27017
export GCP_PROJECT_ID=your-project
export PUBSUB_SUBSCRIPTION=er-records-sub
export SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/...
export SQS_DLQ_URL=https://sqs.us-east-1.amazonaws.com/...-dlq
python -m src.pipeline.ingestion_pipeline
```

## Metrics (Prometheus)

| Metric | Type | Description |
|---|---|---|
| `er_messages_processed_total` | Counter | Messages successfully ingested, by source |
| `er_messages_failed_total` | Counter | Messages routed to DLQ, by source + error |
| `er_duplicates_skipped_total` | Counter | Duplicate messages dropped |
| `er_processing_duration_seconds` | Histogram | Per-message processing latency |
| `er_batch_write_size` | Histogram | MongoDB bulk write batch sizes |
| `er_pipeline_active` | Gauge | Pipeline liveness (1 = running) |

Prometheus endpoint exposed at `:8000/metrics`.

## MongoDB Schema

Records are stored in `er_records` collection, sharded by `source_region`:

```json
{
  "_id": "<record_id>",
  "record_type": "admission | discharge | lab_result | medication | diagnosis",
  "patient_id": "...",
  "source_region": "us-east-1 | us-west-2 | eu-west-1",
  "payload": { ... },
  "ingested_at": "2025-01-01T00:00:00Z",
  "schema_version": "1.0",
  "checksum": "sha256:..."
}
```

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```
