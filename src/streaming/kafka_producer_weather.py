"""src/streaming/kafka_producer_weather.py."""

from collections.abc import Generator
import json
import os
import time
from typing import Final
import urllib.request

from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger

from streaming.data_validation.data_contract_weather import validate_weather_record

LOG = get_logger("PRODUCER-WEATHER", level="INFO")

MESSAGE_INTERVAL_SECONDS: Final[float] = 1.5
WEATHER_TOPIC: Final[str] = os.getenv("KAFKA_TOPIC_WEATHER", "streaming-05-weather")


def generate_weather_messages() -> Generator[dict[str, str]]:
    """Fetch Honolulu forecast and stream it hour by hour."""
    LOG.info("Fetching live weather forecast for Honolulu from Open-Meteo...")
    url = "https://api.open-meteo.com/v1/forecast?latitude=21.3069&longitude=-157.8583&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&temperature_unit=fahrenheit"

    with urllib.request.urlopen(url) as response:  # noqa: S310
        data = json.loads(response.read().decode())
        hourly = data["hourly"]
        times = hourly["time"]
        temps = hourly["temperature_2m"]
        humids = hourly["relative_humidity_2m"]
        winds = hourly["wind_speed_10m"]

        # Stream up to 48 hours of forecast
        for i in range(min(48, len(times))):
            yield {
                "location": "Honolulu, HI",
                "timestamp": str(times[i]),
                "temperature_f": str(temps[i]),
                "humidity_pct": str(humids[i]),
                "wind_speed_mph": str(winds[i]),
            }


def main() -> None:
    """Run the main producer loop for weather data."""
    LOG.info("Starting Weather Producer...")
    settings = KafkaSettings.from_env()
    settings.topic = WEATHER_TOPIC  # Override to avoid mixing with sales data

    verify_connection(settings)
    prepare_producer_topic(settings)
    producer = create_producer(settings)

    sent_count = 0
    rejected_count = 0

    try:
        for message in generate_weather_messages():
            result = validate_weather_record(message)

            if not result.is_valid:
                rejected_count += 1
                LOG.warning(f"REJECTED: {result.errors}")
                continue

            key = "Honolulu"
            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(
                f"SENT: {message['timestamp']} | Temp: {message['temperature_f']}F"
            )
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except Exception as error:
        LOG.error(f"Producer error: {error}")
    finally:
        producer.flush()
        LOG.info("========================")
        LOG.info(f"Producer Summary: Sent {sent_count}, Rejected {rejected_count}")
        LOG.info("========================")


def verify_connection(settings: KafkaSettings):
    """Verify Kafka connection before starting."""
    verify_kafka_connection(settings)


if __name__ == "__main__":
    main()
