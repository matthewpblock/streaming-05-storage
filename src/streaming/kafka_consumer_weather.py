"""src/streaming/kafka_consumer_weather.py."""

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger

from streaming.data_validation.data_contract_weather import (
    CONSUMED_FIELDNAMES,
    validate_weather_record,
)
from streaming.storage.storage_weather import (
    init_db,
    log_storage_summary,
    write_valid_record,
)

LOG = get_logger("CONSUMER-WEATHER", level="INFO")

TIMEOUT_SECONDS: Final[float] = 10.0
MAX_MESSAGES: Final[int] = 100
WEATHER_TOPIC: Final[str] = os.getenv("KAFKA_TOPIC_WEATHER", "streaming-05-weather")

OUTPUT_DIR: Final[Path] = Path.cwd() / "data" / "output"
OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "honolulu_weather.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "weather.duckdb"


def enrich_weather(record: dict[str, Any]) -> dict[str, Any]:
    """Add derived metric fields."""
    enriched = dict(record)
    temp_f = float(enriched["temperature_f"])
    humid = int(enriched["humidity_pct"])

    # Enrich 1: Convert to Celsius
    enriched["temp_c"] = str(round((temp_f - 32) * 5.0 / 9.0, 2))

    # Enrich 2: Define "comfortable" parameters (70-82F, under 70% humidity)
    is_comfy = 70.0 <= temp_f <= 82.0 and humid < 70
    enriched["is_comfortable"] = "true" if is_comfy else "false"

    return enriched


def main() -> None:
    """Run the main consumer loop for weather data."""
    LOG.info("Starting Weather Consumer...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    init_db(OUTPUT_DB)

    settings = KafkaSettings.from_env()
    settings.topic = WEATHER_TOPIC
    verify_kafka_connection(settings)

    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [TopicPartition(p.topic, p.partition, OFFSET_BEGINNING) for p in partitions]
        ),
    )

    consumed_count = 0

    LOG.info("Consuming messages...")
    try:
        while consumed_count < MAX_MESSAGES:
            row = consume_kafka_message(consumer, timeout_seconds=TIMEOUT_SECONDS)
            if row is None:
                LOG.info("No message received. Exiting.")
                break

            # Validate
            result = validate_weather_record(row)
            if not result.is_valid:
                LOG.warning(f"REJECTED: {result.errors}")
                continue

            # Enrich
            enriched = enrich_weather(row)

            # Store in DB
            write_valid_record(OUTPUT_DB, enriched)

            # Store in CSV
            append_csv_row(
                path=OUTPUT_CSV,
                row={f: enriched.get(f, "") for f in CONSUMED_FIELDNAMES},
                fieldnames=CONSUMED_FIELDNAMES,
            )

            consumed_count += 1
            LOG.info(
                f"PROCESSED: {enriched['timestamp']} | Temp: {enriched['temperature_f']}F "
                f"({enriched['temp_c']}C) | Comfy: {enriched['is_comfortable']}"
            )

    except KeyboardInterrupt:
        LOG.info("Stopped by user.")
    finally:
        consumer.close()
        LOG.info("========================")
        LOG.info(f"Consumer Summary: Processed {consumed_count} forecasts.")
        log_storage_summary(OUTPUT_DB)
        LOG.info("========================")


if __name__ == "__main__":
    main()
