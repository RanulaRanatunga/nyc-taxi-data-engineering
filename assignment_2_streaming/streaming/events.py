import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

MIN_LOCATION_ID, MAX_LOCATION_ID = 1, 265


class InvalidEventError(ValueError):
    pass

def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        raise InvalidEventError(f"expected ISO timestamp, got {value!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _optional_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return int(value)


def _float(value: Any, default: Optional[float] = None) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        if default is None:
            raise InvalidEventError("missing required numeric value")
        return default
    result = float(value)
    if math.isnan(result) or math.isinf(result):
        raise InvalidEventError(f"invalid numeric value {value!r}")
    return result


@dataclass
class TripEvent:
    trip_id: str
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pulocation_id: int
    dolocation_id: int
    trip_distance: float
    fare_amount: float
    tip_amount: float
    total_amount: float
    vendor_id: Optional[int] = None
    passenger_count: Optional[int] = None
    payment_type: Optional[int] = None
    produced_at: Optional[float] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None

    @property
    def event_ts(self) -> float:
        return self.pickup_datetime.replace(tzinfo=timezone.utc).timestamp()

    @property
    def duration_min(self) -> float:
        return (self.dropoff_datetime - self.pickup_datetime).total_seconds() / 60.0

    @classmethod
    def from_dict(cls, data: dict) -> "TripEvent":
        try:
            event = cls(
                trip_id=str(data["trip_id"]).strip(),
                pickup_datetime=_to_datetime(data["pickup_datetime"]),
                dropoff_datetime=_to_datetime(data["dropoff_datetime"]),
                pulocation_id=int(data["pulocation_id"]),
                dolocation_id=int(data["dolocation_id"]),
                trip_distance=_float(data["trip_distance"]),
                fare_amount=_float(data["fare_amount"]),
                tip_amount=_float(data.get("tip_amount"), default=0.0),
                total_amount=_float(data["total_amount"]),
                vendor_id=_optional_int(data.get("vendor_id")),
                passenger_count=_optional_int(data.get("passenger_count")),
                payment_type=_optional_int(data.get("payment_type")),
                produced_at=_float(data.get("produced_at"), default=0.0) or None,
            )
        except InvalidEventError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidEventError(f"{type(exc).__name__}: {exc}") from exc

        if not event.trip_id or len(event.trip_id) > 64:
            raise InvalidEventError("trip_id must be 1-64 characters")
        if event.dropoff_datetime < event.pickup_datetime:
            raise InvalidEventError("dropoff_datetime is before pickup_datetime")
        if event.trip_distance < 0:
            raise InvalidEventError("trip_distance is negative")
        if not MIN_LOCATION_ID <= event.pulocation_id <= MAX_LOCATION_ID:
            raise InvalidEventError(f"pulocation_id {event.pulocation_id} outside 1-265")
        return event

    @classmethod
    def from_json(cls, raw: bytes) -> "TripEvent":
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidEventError(f"invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise InvalidEventError("payload is not a JSON object")
        return cls.from_dict(payload)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["pickup_datetime"] = self.pickup_datetime.isoformat()
        data["dropoff_datetime"] = self.dropoff_datetime.isoformat()
        data.pop("kafka_partition")
        data.pop("kafka_offset")
        return data
