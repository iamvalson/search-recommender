# Search Recommendation System

A distributed, real-time search recommendation engine that re-ranks search
results based on user click behaviour. Built to demonstrate production-grade
distributed systems patterns using Kafka, Redis, and PostgreSQL.

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           Docker Network                 │
                        │                                          │
  User Click            │  ┌──────────────┐                        │
──────────────────────► │  │ Click Tracker│                        │
  POST /click           │  │  (FastAPI)   │                        │
  :8000                 │  │   :8000      │                        │
                        │  └──────┬───────┘                        │
                        │         │ publish                        │
                        │         ▼                                │
                        │  ┌──────────────┐                        │
                        │  │    Kafka     │                        │
                        │  │click-events  │                        │
                        │  │   topic      │                        │
                        │  └──────┬───────┘                        │
                        │         │ consume                        │
                        │         ▼                                │
                        │  ┌──────────────┐    upsert score        │
                        │  │   Stream     │──────────────────►     │
                        │  │  Processor   │   ┌────────────┐       │
                        │  │  (Python)    │   │ PostgreSQL │       │
                        │  └──────────────┘   │click_scores│       │
                        │                     └─────┬──────┘       │
                        │                           │ read scores  │
                        │  ┌──────────────┐         │              │
  User Search           │  │  Search API  │◄────────┘              │
──────────────────────► │  │  (FastAPI)   │                        │
  GET /search           │  │   :8001      │◄───────────────────    │
  :8001                 │  └──────────────┘   ┌────────────┐       │
                        │         │           │   Redis    │       │
                        │         └──────────►│   Cache    │       │
                        │          cache hit  │   :6379    │       │
                        │                     └────────────┘       │
                        └─────────────────────────────────────────┘
```

## How It Works

1. **Click Tracking** — when a user clicks a search result, a click event
   is published to a Kafka topic called `click-events`

2. **Stream Processing** — a Kafka consumer reads click events in real time
   and increments scores in PostgreSQL using an upsert pattern

3. **Caching** — the Search API checks Redis before hitting PostgreSQL.
   Hot queries are served in <1ms. Cache TTL is 60 seconds.

4. **Re-ranking** — search results are sorted by click score before being
   returned, so popular results bubble to the top over time

## Key Design Decisions

**Why Kafka instead of a simple HTTP call?**
Kafka decouples the click tracker from the score updater. If the stream
processor goes down, click events are durably stored and replayed when
it recovers. A direct HTTP call would lose that data.

**Why Redis for caching?**
PostgreSQL is optimised for writes and complex queries, not high-volume
point reads. Redis serves cached scores in ~0.5ms vs ~5-10ms from
PostgreSQL — a 10-20x improvement at scale.

**Why connection pooling?**
Opening a new database connection per request is expensive (~5-10ms
overhead). A pool of 2-10 reusable connections eliminates that cost.

## Services

| Service       | Port | Responsibility                            |
| ------------- | ---- | ----------------------------------------- |
| click_tracker | 8000 | Receives click events, publishes to Kafka |
| search_api    | 8001 | Serves re-ranked search results           |
| kafka         | 9092 | Distributed message queue                 |
| zookeeper     | 2181 | Kafka cluster coordination                |
| redis         | 6379 | Score cache (TTL: 60s)                    |
| postgres      | 5432 | Persistent click score storage            |

## Running Locally

**Prerequisites:** Docker Desktop with WSL2

```bash
git clone https://github.com/iamvalson/search-recommender
cd search-recommender/infra
docker compose up -d --build
```

Wait ~30 seconds for all services to become healthy, then:

```bash
# Send a click event
curl -X POST http://localhost:8000/click \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user_1", "query": "python tutorials", "result_id": "result_1"}'

# Search with re-ranked results
curl "http://localhost:8001/search?query=python%20tutorials"

# View system metrics
curl http://localhost:8001/metrics
```

## Observability

| Endpoint            | What it shows                            |
| ------------------- | ---------------------------------------- |
| GET /health (8001)  | Service status, Redis and DB pool health |
| GET /metrics (8001) | Cache hit rate, Kafka lag, top queries   |

## Metrics Observed

- **Cache hit rate:** 57% after light traffic (improves with more traffic)
- **Cache HIT latency:** ~0.5-0.7ms
- **Cache MISS latency:** ~2-10ms
- **Kafka consumer lag:** 0 (processor keeping up in real time)

## Tech Stack

| Layer            | Technology              |
| ---------------- | ----------------------- |
| API Framework    | FastAPI + Uvicorn       |
| Message Queue    | Apache Kafka 7.7.8      |
| Cache            | Redis 8.6               |
| Database         | PostgreSQL 18           |
| Containerisation | Docker + Docker Compose |
| Language         | Python 3.14             |
