from prometheus_client import Counter, Gauge, Histogram, start_http_server

messages_processed = Counter(
    "er_messages_processed_total",
    "Messages successfully ingested",
    ["source"],
)

messages_failed = Counter(
    "er_messages_failed_total",
    "Messages routed to DLQ",
    ["source", "error_type"],
)

duplicates_skipped = Counter(
    "er_duplicates_skipped_total",
    "Duplicate messages dropped by idempotency check",
    ["source"],
)

processing_duration = Histogram(
    "er_processing_duration_seconds",
    "Per-message processing latency",
    ["source"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

batch_write_size = Histogram(
    "er_batch_write_size",
    "MongoDB bulk write batch sizes",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

pipeline_active = Gauge(
    "er_pipeline_active",
    "Pipeline liveness: 1 = running, 0 = stopped",
)

replication_lag = Gauge(
    "er_replication_lag_seconds",
    "Estimated replication lag to each secondary region",
    ["region"],
)

replication_errors = Counter(
    "er_replication_errors_total",
    "Failed replication attempts to secondary regions",
    ["region"],
)


def start_metrics_server(port: int = 8000):
    start_http_server(port)
