# ER-Insight — Multi-Cloud Data Ingestion Pipeline

A fault-tolerant, multi-cloud data ingestion pipeline for high-volume ER (Electronic Records) data, built on **Google Cloud Pub/Sub**, **AWS SQS/SNS**, and **MongoDB**.

## Architecture

```
Raw Sources
    │
    ├── GCP Pub/Sub  ──► PubSubConsumer  ─┐
    │                                     ├──► IngestionPipeline ──► MongoDB
    └── AWS SQS/SNS  ──► SQSConsumer    ─┘
```

- **200K+ daily records** ingested across multi-cloud sources
- **100M+ messages/month** sustained via Pub/Sub + Cloud Tasks
- Cross-region replication and disaster recovery built-in
- Idempotent consumers with dead-letter queues for exactly-once delivery

## Project Structure

```
src/
├── consumers/
│   ├── base_consumer.py      # Idempotent base consumer with DLQ support
│   ├── pubsub_consumer.py    # GCP Pub/Sub consumer
│   └── sqs_consumer.py       # AWS SQS/SNS consumer
├── schema/
│   └── er_record.py          # MongoDB document schema for ER record types
└── pipeline/
    └── ingestion_pipeline.py # Main orchestration pipeline
```

## Tech Stack

- **Messaging**: Google Cloud Pub/Sub, AWS SQS/SNS, Cloud Tasks
- **Storage**: MongoDB (with sharding for heterogeneous ER record types)
- **Languages**: Python, Java
- **Reliability**: Idempotent consumers, DLQ, retry logic, cross-region replication

## Setup

```bash
pip install -r requirements.txt
cp config/config.yaml.example config/config.yaml
# Fill in GCP project ID, AWS credentials, MongoDB URI
python -m src.pipeline.ingestion_pipeline
```
