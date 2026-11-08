from prometheus_client import Counter, Gauge, Histogram

CONSUMED = Counter("claims_events_consumed_total", "Messages read from Kafka")
FAILED = Counter("claims_events_failed_total", "Failed processing attempts", ["reason"])
DLQ = Counter("claims_events_dlq_total", "Messages sent to the dead-letter topic", ["reason"])
DUPLICATES = Counter("claims_events_duplicates_total", "Events skipped by event_id dedup")
APPLIED = Counter("claims_events_applied_total", "Events applied to claim_status")
LAG = Gauge("claims_consumer_lag", "Messages behind the log end offset", ["partition"])
LATENCY = Histogram(
    "claims_event_processing_seconds",
    "Time to fully handle one message, retries included",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
