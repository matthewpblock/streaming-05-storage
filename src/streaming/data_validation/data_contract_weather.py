"""src/streaming/data_validation/data_contract_weather.py."""

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import validate_required_fields

WEATHER_REQUIRED_FIELDS: Final[list[str]] = [
    "location",
    "pulled_timestamp",
    "predicting_timestamp",
    "temperature_f",
    "humidity_pct",
    "wind_speed_mph",
    "wind_direction_deg",
]

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *WEATHER_REQUIRED_FIELDS,
    "temp_c",
    "is_comfortable",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_FIELDNAMES: Final[list[str]] = [
    *WEATHER_REQUIRED_FIELDS,
    "validation_errors",
]


def validate_weather_record(record: DataRecordDict) -> ValidationResult:
    """Validate one weather record against our data contract."""
    errors: list[str] = []

    errors.extend(
        validate_required_fields(record=record, required_fields=WEATHER_REQUIRED_FIELDS)
    )

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    try:
        temp = float(record["temperature_f"])
        if temp < -50 or temp > 150:
            errors.append(f"Temperature out of bounds: {temp}")
    except ValueError:
        errors.append(f"Invalid temperature format: {record['temperature_f']}")

    try:
        humidity = int(record["humidity_pct"])
        if humidity < 0 or humidity > 100:
            errors.append(f"Humidity out of bounds: {humidity}")
    except ValueError:
        errors.append(f"Invalid humidity format: {record['humidity_pct']}")

    try:
        direction = int(record["wind_direction_deg"])
        if direction < 0 or direction > 360:
            errors.append(f"Wind direction out of bounds: {direction}")
    except ValueError:
        errors.append(f"Invalid wind direction format: {record['wind_direction_deg']}")

    has_errors = bool(errors)
    is_result_valid = not has_errors

    return ValidationResult(is_valid=is_result_valid, errors=errors)


def keep_weather_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required weather fields."""
    return {field: row.get(field, "") for field in WEATHER_REQUIRED_FIELDS}
