from fastapi import FastAPI
from psycopg2 import pool
from confluent_kafka.admin import AdminClient
from collections import defaultdict
import redis
import json
import logging
import os
import time


# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

app = FastAPI(title="Search API")

# Memory Metric Store
metrics = {
    "cache_hits": 0,
    "cache_misses": 0,
    "query_counts": defaultdict(int)
}

# Redis connection
def get_cache():
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=6379,
            decode_responses=True, # return string instead of bits
            socket_connect_timeout=2 # Fails fast if redis is down
        )
        client.ping()
        return client
    except Exception as e:
        log.warning(f"Redis unavailable: {e}. Running without cache.")
        return None

cache = get_cache()


# Postgres connection pool
db_pool = pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=10,
    host=os.getenv("POSTGRES_HOST", "localhost"),
    database=os.getenv("POSTGRES_DB", "recommender"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "secret")
)



def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)


# Mock search results
def get_mock_results(query:str) -> list:
    return[
        {"result_id": f"result_{i}", "title": f"Result {i} for '{query}'"}
        for i in range(1, 6)
    ]


# Fetch score
def fetch_scores_from_db(query: str) -> dict:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT result_id, score
                    FROM click_scores
                    WHERE query = %s
                """, (query,)
            )
            rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}
    finally:
        release_db_connection(conn)


def get_scores(query: str) -> dict:
    cache_key = f"scores:{query}"

    # Checking redis cache
    if cache:
        try:
            cached = cache.get(cache_key)
            if cached:
                metrics["cache_hits"] += 1
                log.info(f"Cache HIT for '{query}'")
                return json.loads(cached)
        except Exception as e:
            log.warning(f"Redis read failed: {e}. Falling back to DB.")
    
    # Cache miss - go to PostgreSQL
    metrics["cache_misses"] += 1
    log.info(f"Cache MISS for '{query}' - fetching from db")
    scores = fetch_scores_from_db(query)

    # Store in Redis with 60 sec TTL
    if cache:
        try:
            cache.setex(cache_key, 60, json.dumps(scores))
        except Exception as e:
            log.warning(f"Redis write failed: {e}. Continuing without caching.")

    return scores



# Kafka consumer lag
def get_kafka_consumer_lag() -> int:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        admin = AdminClient({
            "bootstrap.servers": bootstrap_servers
        })

        # Get total message in topic
        topic_metadata = admin.list_topics(timeout=5)
        if "click_events" not in topic_metadata.topics:
            log.info("Topic 'click-events' not found yet.")
            return 0
        
        # Get committed offset for consumer group
        from confluent_kafka import Consumer, TopicPartition
        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "stream-processor-group"
        })

        partitions = [
            TopicPartition("click-events", p)
            for p in topic_metadata["click-events"].partitions
        ]

        # High watermark = latest message offset in partition
        committed = consumer.committed(partitions, timeout=5)
        lag = 0
        for tp in committed:
            _, high = consumer.get_watermark_offsets(tp, timeout=5)
            committed_offset = tp.offset if tp.offset >= 0 else 0
            lag += high - committed_offset
        
        consumer.close()
        return lag
    except Exception as e:
        log.warning(f"Could not fetch kafka lag: {e}")
        return -1


# Search endpoint 
@app.get("/search")
def search(query: str):
    start = time.time()

    # Track query popularity
    metrics["query_counts"][query] += 1

    # Get mock data
    results = get_mock_results(query)

    scores = get_scores(query)

    # Re-rank results by score (highest first)
    results.sort(
        key=lambda r: scores.get(r["result_id"], 0),
        reverse=True
    )

    # Add score to each result for visibility
    for result in results:
        result["score"] = scores.get(result["result_id"], 0)

    duration_ms = round((time.time() - start) * 1000, 2)
    log.info(f"Search for '{query}' completed in {duration_ms}ms")

    return {
        "query": query,
        "results": results,
        "latency_ms": duration_ms
    }

@app.get("/metrics")
def get_metrics():
    total = metrics["cache_hits"] + metrics["cache_misses"]
    hit_rate = round(metrics["cache_hits"] / total, 2) if total > 0 else 0.0

    top_queries = sorted(
        [{"query": q, "searches": c} for q, c in metrics["query_counts"].items()],
        key=lambda x: x["searches"],
        reverse=True
    )[:5]

    return {
        "cache_hits": metrics["cache_hits"],
        "cache_misses": metrics["cache_misses"],
        "total_searches": total,
        "cache_hit_rate": hit_rate,
        "kafka_consumer_lag": get_kafka_consumer_lag(),
        "top_queries": top_queries
    }

# Health check
@app.get("/health")
def health():
    return {"status": "healthy",
            "redis": "connected" if cache else "unavailable", "db_pool": f"{db_pool.minconn}-{db_pool.maxconn} connections"}