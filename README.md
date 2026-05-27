# Customer Event Flink Pipeline

A PyFlink streaming pipeline that reads retail customer events from Kafka, computes per-customer spend aggregations using a **5-minute tumbling event-time window with watermarks**, and writes results to stdout and a CSV file.

---

## Architecture

```
  Kafka                      Flink DataStream Pipeline                    Sinks
  ─────                      ──────────────────────────                   ─────
                                                                       ┌──────────────┐
┌──────────────┐             ┌──────────────────────────────┐          │              │
│              │  JSON str   │                              │ ─────▶   │  stdout      │
│  customer-   │ ──────────▶ │  KafkaSource                 │          │  (docker     │
│  events      │             │       │                      │          │   logs)      │
│  (4 parts)   │             │       ▼                      │          └──────────────┘
│              │             │  ParseEventFunction          │
└──────────────┘             │  (str → dict, error filter)  │
                             │       │                      │
  Producer                  │       ▼                      │
  ────────                  │  assign_timestamps_           │          ┌──────────────┐
  ~10 events/sec            │  and_watermarks               │          │              │
  10% late (30–60 s)        │  WatermarkStrategy            │          │  CSV file    │
                             │  BoundedOutOfOrderness(30 s) │ ─────▶   │  /tmp/flink- │
                             │       │                      │          │  output/     │
                             │       ▼                      │          │  results.csv │
                             │  key_by(customer_id)         │          └──────────────┘
                             │       │                      │
                             │       ▼                      │
                             │  TumblingEventTimeWindows    │
                             │  (5 minutes)                 │
                             │       │                      │
                             │       ▼                      │
                             │  SpendWindowFunction         │
                             │  (count, spend, purchases)   │
                             │                              │
                             └──────────────────────────────┘
```

---

## Event-Time vs Processing Time

| | Event Time | Processing Time |
|---|---|---|
| Clock source | Field in the event payload (`event_timestamp`) | Wall clock when Flink processes the record |
| Reproducibility | Same result on replay | Different result on replay |
| Late-event handling | Explicit via watermarks | No concept of "late" |

**Why event time matters here:**  
A customer browses on a mobile app, loses connectivity for 45 seconds, and comes back online. Those buffered events arrive at Kafka 45 seconds late. With **processing time**, they would fall into the *next* 5-minute window, inflating the next period's spend and zeroing out the actual purchase window. With **event time**, Flink knows when the event *happened* and places it in the correct window — as long as it arrives before the watermark passes.

---

## Watermarks

A watermark is a monotonically-increasing timestamp that Flink inserts into the stream to signal: *"I am confident that no event with a timestamp earlier than T will arrive."*

When a watermark with value `W` passes through the pipeline:
- All windows with `end ≤ W` are considered complete and are triggered
- Any event arriving after that watermark with `timestamp < W` is **late** and, by default, dropped

**Why 30-second bounded out-of-orderness?**  
`WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))` generates watermarks as:

```
watermark = max_event_time_seen − 30 seconds
```

This gives the pipeline a 30-second grace period to receive out-of-order events before closing a window. The value was chosen to match the observed maximum network delay between the mobile app and Kafka. Choosing a larger value (e.g., 5 minutes) would increase result latency; choosing a smaller value (e.g., 5 seconds) would cause more events to be counted as late.

---

## Window Types

| Type | Definition | Use case |
|---|---|---|
| **Tumbling** | Fixed-size, non-overlapping windows | Hourly sales totals, per-minute error counts |
| **Sliding** | Fixed-size windows that advance by a smaller step (overlap) | 5-min spend with a 1-min refresh rate |
| **Session** | Variable-size windows that close after a gap of inactivity | User session analysis, cart abandonment |

This pipeline uses **tumbling** windows: a clean 5-minute boundary that produces one row per customer per window with no double-counting.

---

## Project Structure

```
customer-event-flink/
├── Dockerfile                      # Custom Flink image with Kafka connector JAR + PyFlink
├── docker-compose.yml              # Flink cluster + Kafka + Zookeeper
├── requirements.txt
├── README.md
├── producer/
│   └── produce_events.py           # Kafka producer (~10 events/sec, 10% late arrivals)
└── pipeline/
    ├── customer_event_job.py       # Job entrypoint — pipeline wiring
    └── window_function.py          # TimestampAssigner + ProcessWindowFunction
```

---

## How to Run Locally

### 1. Start the cluster

```bash
docker-compose up --build -d
```

This starts: Zookeeper, Kafka, Flink JobManager (UI on :8081), Flink TaskManager.  
Wait ~30 seconds for Kafka to be ready.

### 2. Start the producer (on your host machine)

```bash
pip install confluent-kafka faker
python producer/produce_events.py
```

The producer targets `localhost:9092` by default.  
You should see log lines like:

```
2024-01-15 10:00:01  INFO     Producing to localhost:9092/customer-events at ~10 events/sec  (10% late arrivals)
2024-01-15 10:00:11  INFO     Produced 100 events (last customer: cust_AB12CD)
```

### 3. Submit the PyFlink job

```bash
docker exec -it customer-event-flink-jobmanager-1 \
  flink run \
    --python /opt/flink/jobs/customer_event_job.py \
    --pyFiles /opt/flink/jobs/window_function.py
```

The job will appear in the Flink Web UI at [http://localhost:8081](http://localhost:8081).

### 4. Watch the output

**Stdout** (via TaskManager logs):
```bash
docker logs -f customer-event-flink-taskmanager-1
```

**CSV file** (inside the Docker volume):
```bash
docker exec customer-event-flink-taskmanager-1 cat /tmp/flink-output/results.csv
```

---

## Sample Output

Windows fire every 5 minutes as the event-time watermark advances.

**stdout:**
```
{'customer_id': 'cust_AB12CD', 'window_start': '2024-01-15T10:00:00+00:00', 'window_end': '2024-01-15T10:05:00+00:00', 'event_count': 18, 'purchase_count': 5, 'total_spend': 847.32}
{'customer_id': 'cust_EF34GH', 'window_start': '2024-01-15T10:00:00+00:00', 'window_end': '2024-01-15T10:05:00+00:00', 'event_count': 12, 'purchase_count': 3, 'total_spend': 412.15}
{'customer_id': 'cust_IJ56KL', 'window_start': '2024-01-15T10:00:00+00:00', 'window_end': '2024-01-15T10:05:00+00:00', 'event_count':  9, 'purchase_count': 2, 'total_spend': -43.50}
```

**CSV:**
```
customer_id,window_start,window_end,event_count,purchase_count,total_spend
cust_AB12CD,2024-01-15T10:00:00+00:00,2024-01-15T10:05:00+00:00,18,5,847.32
cust_EF34GH,2024-01-15T10:00:00+00:00,2024-01-15T10:05:00+00:00,12,3,412.15
cust_IJ56KL,2024-01-15T10:00:00+00:00,2024-01-15T10:05:00+00:00,9,2,-43.50
```

`total_spend` is negative for customers whose only activity in the window was returns.

---

## Key Design Decisions

**DataStream API over Flink SQL:** The DataStream API exposes the full power of the Flink runtime — custom `ProcessWindowFunction`, explicit watermark configuration, typed key selectors. Flink SQL is excellent for ad-hoc queries but hides these mechanisms behind abstraction, making it harder to reason about late-event behaviour and state management.

**`ProcessWindowFunction` over `ReduceFunction`:** A `ReduceFunction` aggregates incrementally and is more memory-efficient, but only the final accumulator is available at emit time — you cannot access `context.window().start` to attach the window boundaries to the output. `ProcessWindowFunction` buffers all elements but provides full context access, which is necessary for the output schema.

**Dual listeners in Kafka:** The producer runs on the host machine (`localhost:9092`) while the Flink job runs inside the Docker network (`kafka:29092`). Docker Compose configures both advertised listener addresses so both can reach the same broker.

**`KafkaOffsetsInitializer.earliest()`:** Ensures the job processes events that were produced before the job started. In production this would be replaced with `committed()` so that a restarting job resumes from its last committed offset rather than reprocessing the entire topic.

---

## Stack

Python · PyFlink 1.18 · Apache Kafka · Confluent Kafka Python · Docker Compose · Faker
