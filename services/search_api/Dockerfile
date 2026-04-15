FROM python:3.14-slim

WORKDIR /app

RUN pip install fastapi uvicorn psycopg2-binary redis confluent_kafka

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]