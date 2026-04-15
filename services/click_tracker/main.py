from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from confluent_kafka import Producer
import json
import time
import os


app = FastAPI(title="Click Tracker")


producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
})


# Data model
class ClickEvent(BaseModel):
    user_id: str
    query: str
    result_id: str

# Confirmation callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} partition {msg.partition()}")

# Endpoint
@app.post("/click")
def track_click(event: ClickEvent, background_tasks: BackgroundTasks):
    try:
        payload = {
            "user_id": event.user_id,
            "query": event.query,
            "result_id": event.result_id,
            "timestamp": time.time()
        }

        # Publisher to Kafka topic "click-events"
        def send_to_kafka(data):
            producer.produce(
                topic="click-events",
                key=data["user_id"],
                value=json.dumps(data),
                callback=delivery_report
            )

            producer.flush()

        background_tasks.add_task(send_to_kafka, payload)

        return {"status": "ok", "message": "Click event recorded" }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Health check
@app.get("/health")
def health():
    return {"status": "healthy" }