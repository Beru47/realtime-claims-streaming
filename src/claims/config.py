import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    database_url: str
    topic: str
    dlq_topic: str
    group_id: str
    metrics_port: int
    max_attempts: int
    backoff_base: float
    backoff_cap: float


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        bootstrap=env("KAFKA_BOOTSTRAP", "localhost:9092"),
        database_url=env("DATABASE_URL", "postgresql://claims:claims@localhost:5432/claims"),
        topic=env("CLAIMS_TOPIC", "claims.events"),
        dlq_topic=env("CLAIMS_DLQ_TOPIC", "claims.events.dlq"),
        group_id=env("CLAIMS_GROUP_ID", "claims-sink"),
        metrics_port=int(env("METRICS_PORT", "8001")),
        max_attempts=int(env("MAX_ATTEMPTS", "5")),
        backoff_base=float(env("BACKOFF_BASE_SECONDS", "0.5")),
        backoff_cap=float(env("BACKOFF_CAP_SECONDS", "10")),
    )
