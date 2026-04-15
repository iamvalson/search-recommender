from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
import psycopg2
import json
import os
import time
import logging


# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "recommender"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "secret")
    )


# Create table if non-existent
def setup_database(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS click_scores (
                    query           TEXT NOT NULL,
                    result_id       TEXT NOT NULL,
                    score           INTEGER DEFAULT 1,
                    updated_at      TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY     (query, result_id)
                )
            """
        )
        conn.commit()
    log.info("Database table ready")


# Process single click event

def process_click(conn, event: dict):
    query = event["query"]
    result_id = event["result_id"]

    with conn.cursor() as cur:
        cur.execute(
            """
                INSERT INTO click_scores (query, result_id, score)
                VALUES (%s, %s, 1)
                ON CONFLICT (query, result_id)
                DO UPDATE SET
                    score = click_scores.score + 1,
                    updated_at = NOW()
            """, (query, result_id)
        )
        conn.commit()
    log.info(f"Updated score for query='{query}' result='{result_id}'")


def wait_for_kafka(bootstrap_servers, retries=10, delay=5):
    """Actively probe Kafka until it's ready instead of blindly sleeping."""
    for attempt in range(retries):
        try:
            client = AdminClient({"bootstrap.servers": bootstrap_servers})
            client.list_topics(timeout=5)
            log.info("Kafka is ready!")
            return
        except Exception:
            log.error(f"Kafka not ready (attempt {attempt + 1}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Kafka never became ready after maximum retries")


# Consumer loop
def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Waiting for Kafka and Postgres to be ready
    wait_for_kafka(bootstrap_servers)

    # Connect to PostgreSQL
    conn = get_db_connection()
    setup_database(conn)


    # Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "stream-processor-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(["click-events"])
    log.info("Listening for event clicks...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue # Reached end of partition, keep waiting
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic not ready yet, retrying...")
                    time.sleep(2)
                    continue
                else:
                    log.error(f"kafka error: {msg.error()}")
                    break

            # Decode and process the message
            event = json.loads(msg.value().decode("utf-8"))
            log.info(f"Recieved: {event}")
            process_click(conn, event)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()