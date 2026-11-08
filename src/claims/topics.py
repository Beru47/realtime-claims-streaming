from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topics(bootstrap: str, names: list[str], partitions: int = 3) -> None:
    """Create topics if missing. Idempotent: 'already exists' is not an error."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(n, partitions, 1) for n in names])
    for name, fut in futures.items():
        try:
            fut.result(timeout=30)
        except Exception as exc:  # confluent raises KafkaException with TOPIC_ALREADY_EXISTS
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"cannot create topic {name}: {exc}") from exc
