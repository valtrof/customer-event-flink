"""
Window aggregation logic for the customer spend pipeline.

Separated from the job file so the classes can be tested independently
and imported without triggering Flink environment initialisation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import ProcessWindowFunction


def _ms_to_iso(epoch_ms: int) -> str:
    """Convert a Flink epoch-millisecond timestamp to an ISO-8601 string."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat()


class CustomerEventTimestampAssigner(TimestampAssigner):
    """
    Extracts the business event time from the 'event_timestamp' field.

    Flink needs timestamps in epoch milliseconds to drive event-time
    processing and watermark advancement.  We pull this from the event
    payload rather than using Kafka record metadata, which gives us true
    business time semantics even when events are replayed or delayed.
    """

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        try:
            raw = value["event_timestamp"]
            # Handle both offset-aware ("2024-01-01T12:00:00+00:00") and
            # naive ISO strings produced by datetime.isoformat().
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except (KeyError, ValueError):
            # Fall back to Kafka record timestamp so the event is not dropped;
            # in production this would route to a dead-letter topic.
            return record_timestamp


class SpendWindowFunction(ProcessWindowFunction):
    """
    Aggregates customer events inside a tumbling 5-minute event-time window.

    Flink calls process() once per (key, window) pair after the watermark
    passes the window's end boundary.  All events that arrived within the
    window's [start, end) interval are passed as 'elements'.

    Output schema per window:
        customer_id    : str
        window_start   : ISO-8601 UTC
        window_end     : ISO-8601 UTC
        event_count    : int   — total events in the window
        purchase_count : int   — events where event_type == 'purchase'
        total_spend    : float — sum of amount for purchases (returns are negative)
    """

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[dict],
    ) -> Iterable[dict]:
        event_count = 0
        purchase_count = 0
        total_spend = 0.0

        for event in elements:
            event_count += 1
            if event.get("event_type") == "purchase":
                purchase_count += 1
                total_spend += float(event.get("amount", 0.0))
            elif event.get("event_type") == "return":
                # Returns carry a negative amount; include in spend so net
                # revenue reflects refunds.
                total_spend += float(event.get("amount", 0.0))

        window = context.window()
        yield {
            "customer_id": key,
            "window_start": _ms_to_iso(window.start),
            "window_end": _ms_to_iso(window.end),
            "event_count": event_count,
            "purchase_count": purchase_count,
            "total_spend": round(total_spend, 2),
        }
