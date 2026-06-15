"""src/streaming/kafka_producer_weather.py."""

from collections.abc import Generator
from datetime import UTC, datetime
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
    url = "https://api.open-meteo.com/v1/forecast?latitude=21.3069&longitude=-157.8583&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m&temperature_unit=fahrenheit&forecast_days=10"

    with urllib.request.urlopen(url) as response:  # noqa: S310
        data = json.loads(response.read().decode())
        hourly = data["hourly"]
        times = hourly["time"]
        temps = hourly["temperature_2m"]
        humids = hourly["relative_humidity_2m"]
        winds = hourly["wind_speed_10m"]
        dirs = hourly["wind_direction_10m"]

        # Mark the exact time we pulled this 10-day forecast dataset
        pulled_timestamp = datetime.now(UTC).isoformat(timespec="seconds")

        # Stream all 10 days of forecast (240 hours)
        for i in range(len(times)):
            yield {
                "location": "Honolulu, HI",
                "pulled_timestamp": pulled_timestamp,
                "predicting_timestamp": str(times[i]),
                "temperature_f": str(temps[i]),
                "humidity_pct": str(humids[i]),
                "wind_speed_mph": str(winds[i]),
                "wind_direction_deg": str(dirs[i]),
            }


def main() -> None:
    """Run the main producer loop for weather data."""
    LOG.info("Starting Weather Producer...")
    os.environ["KAFKA_TOPIC"] = (
        WEATHER_TOPIC  # Override environment variable before loading
    )
    settings = KafkaSettings.from_env()

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
                f"SENT: Pulled {message['pulled_timestamp']} | Predicting {message['predicting_timestamp']} | "
                f"Temp: {message['temperature_f']}F | Wind: {message['wind_speed_mph']}mph @ {message['wind_direction_deg']}deg"
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
