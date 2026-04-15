FROM python:3.14-slim

WORKDIR /app

RUN pip install confluent-kafka psycopg2-binary

COPY main.py .

CMD ["python", "main.py"]