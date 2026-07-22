"""Single source of truth for the clickstream event schema.

Imported by the producer (validates before publish, Step 3b) and by the
Spark jobs (parses/enforces structure from bronze onward, Step 5+). The
contract is defined once here, not redefined independently in each place.
"""

EVENT_TYPES = {"page_view", "add_to_cart", "remove_from_cart", "purchase", "search"}

# (field name, allowed python types, description) — order matches the design doc.
FIELDS = [
    ("event_id", (str,), "UUID, unique per event"),
    ("user_id", (str,), "e.g. user_123"),
    ("session_id", (str,), "UUID, shared across a user's session"),
    ("event_type", (str,), "one of EVENT_TYPES"),
    ("product_id", (str,), "e.g. prod_42"),
    ("price", (int, float), "product price at time of event"),
    ("timestamp", (int, float), "unix epoch seconds, event-time"),
]

FIELD_NAMES = [name for name, _, _ in FIELDS]


class SchemaValidationError(ValueError):
    """Raised when an event does not conform to the shared event schema."""


def validate_event(event: dict) -> None:
    """Validate a single event dict against the shared schema.

    Collects *all* violations (not just the first) into one error so a
    caller logging the rejection sees the full picture. Used at the
    producer boundary so a malformed event is caught at the source instead
    of silently dropped later by silver's null-filtering (Step 6).
    """
    errors = []

    for name, allowed_types, _ in FIELDS:
        if name not in event:
            errors.append(f"missing required field '{name}'")
            continue
        value = event[name]
        # bool is a subclass of int in Python; reject it explicitly so a
        # stray True/False doesn't pass as a valid price/timestamp.
        if isinstance(value, bool) or not isinstance(value, allowed_types):
            errors.append(
                f"field '{name}' has type {type(value).__name__}, "
                f"expected one of {[t.__name__ for t in allowed_types]}"
            )

    if "event_type" in event and event["event_type"] not in EVENT_TYPES:
        errors.append(
            f"field 'event_type' has invalid value {event['event_type']!r}, "
            f"expected one of {sorted(EVENT_TYPES)}"
        )

    extra_fields = set(event) - set(FIELD_NAMES)
    if extra_fields:
        errors.append(f"unexpected extra field(s): {sorted(extra_fields)}")

    if errors:
        raise SchemaValidationError("; ".join(errors))


def to_spark_struct_type():
    """Return the equivalent PySpark StructType for this schema.

    PySpark is imported lazily inside this function so this module has no
    hard dependency on it — the producer (Step 3b) imports this file
    without ever needing PySpark installed.
    """
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    spark_types = {
        "event_id": StringType(),
        "user_id": StringType(),
        "session_id": StringType(),
        "event_type": StringType(),
        "product_id": StringType(),
        "price": DoubleType(),
        "timestamp": DoubleType(),
    }
    return StructType(
        [StructField(name, spark_types[name], nullable=False) for name in FIELD_NAMES]
    )
