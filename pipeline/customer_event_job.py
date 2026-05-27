"""
Customer spend aggregation — PyFlink DataStream job.

Pipeline:
    KafkaSource (JSON strings)
        → ParseEventFunction     (str → dict, with error isolation)
        → assign_timestamps_and_watermarks (event-time, 30 s out-of-orderness)
        → key_by(customer_id)
        → TumblingEventTimeWindows(5 min)
        → SpendWindowFunction    (per-window aggregation)

Submit (no --pyFiles needed — job adds its own directory to sys.path):
    flink run --python /opt/flink/jobs/customer_event_job.py
        → print sink  (stdout)
        → CsvSink     (/tmp/flink-output/results.csv)

Run inside the Flink cluster:
    flink run -py /opt/flink/jobs/customer_event_job.py \
              --pyFiles /opt/flink/jobs/window_function.py
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
from typing import Iterable

# Ensure window_function.py is importable when Flink runs this file from
# any working directory (avoids needing --pyFiles on the CLI).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import Time, TumblingEventTimeWindows

from window_function import CustomerEventTimestampAssigner, SpendWindowFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TOPIC = "customer-events"
GROUP_ID = "flink-spend-aggregator"
OUTPUT_CSV = "/tmp/flink-output/results.csv"

# The JAR ships in the custom Flink image (see Dockerfile).
# Specifying it explicitly here makes the dependency visible in code review.
KAFKA_CONNECTOR_JAR = (
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
)


# ---------------------------------------------------------------------------
# Parse stage
# ---------------------------------------------------------------------------

class ParseEventFunction(MapFunction):
    """
    Deserialises raw Kafka JSON strings into Python dicts.

    Malformed records are returned as sentinel dicts tagged with
    '_parse_error': True so they can be filtered without silently dropping
    data — a pattern borrowed from dead-letter queue semantics.
    """

    def map(self, raw: str) -> dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Unparseable record — routing to error path: %s", raw[:200])
            return {"_parse_error": True, "raw": raw}


# ---------------------------------------------------------------------------
# CSV writer (MapFunction, not SinkFunction)
# ---------------------------------------------------------------------------
# PyFlink's SinkFunction requires a Java-backed implementation (_j_function).
# A MapFunction that writes as a side effect and returns the value unchanged
# is the correct pattern for a pure-Python file sink in PyFlink 1.18.

FIELDNAMES = [
    "customer_id",
    "window_start",
    "window_end",
    "event_count",
    "purchase_count",
    "total_spend",
]


class CsvWriterFunction(MapFunction):
    """
    Writes each windowed result to a CSV file and passes the record through.

    Downstream callers can still chain further operators (e.g. print()) on
    the returned stream, giving both a persisted file and live stdout output
    from a single pass through the data.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def map(self, value: dict) -> dict:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        file_exists = os.path.isfile(self._path) and os.path.getsize(self._path) > 0
        with open(self._path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not file_exists:
                writer.writeheader()
            writer.writerow(value)
        return value


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(env: StreamExecutionEnvironment, bootstrap_servers: str) -> None:
    """
    Assembles the full DataStream pipeline and registers both sinks.

    Keeping this as a pure function (no env.execute()) makes it easy
    to unit-test the graph construction separately from job submission.
    """

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        # Start from the earliest available offset so late-arriving events
        # that were buffered in Kafka before the job started are not skipped.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Read raw JSON strings from Kafka.
    # WatermarkStrategy.no_watermarks() here because we assign event-time
    # watermarks after parsing — we need the payload fields first.
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource[customer-events]",
    )

    # Parse JSON → dict, drop records that failed deserialisation.
    parsed_stream = (
        raw_stream
        .map(ParseEventFunction())
        .filter(lambda e: not e.get("_parse_error", False))
    )

    # Assign event-time timestamps extracted from the 'event_timestamp' field
    # and configure the watermark strategy.
    #
    # for_bounded_out_of_orderness(30 s) tells Flink: "assume any event
    # arriving more than 30 seconds after the current maximum event time is
    # late and can be safely ignored when closing windows."  The value is
    # chosen to cover realistic mobile-app offline buffering (a user who
    # loses connectivity for up to 30 seconds and then flushes events).
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(CustomerEventTimestampAssigner())
    )

    timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    # Key → Window → Aggregate.
    result_stream = (
        timestamped_stream
        .key_by(lambda event: event["customer_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(SpendWindowFunction())
    )

    # Write to CSV (side effect) then print to stdout.
    # CsvWriterFunction passes each record through so .print() still works.
    result_stream.map(CsvWriterFunction(OUTPUT_CSV)).print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()

    # Make the Kafka connector available.  The JAR is baked into the image;
    # the explicit add_jars call documents the runtime dependency clearly.
    env.add_jars(KAFKA_CONNECTOR_JAR)

    # Ship window_function.py to every TaskManager worker process.
    # sys.path.insert() only affects the JobManager submission process;
    # the TaskManager deserialises cloudpickled UDFs in a separate Python
    # worker and needs the module available there too.
    job_dir = os.path.dirname(os.path.abspath(__file__))
    env.add_python_file(os.path.join(job_dir, "window_function.py"))

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    log.info("Connecting to Kafka at %s", bootstrap_servers)

    build_pipeline(env, bootstrap_servers)

    env.execute("customer-spend-5min-tumbling-window")


if __name__ == "__main__":
    main()
