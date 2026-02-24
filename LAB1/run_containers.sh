#!/bin/bash

# Start containers
docker compose up -d --build

# Wait for Kafka to be ready (simple sleep, adjust if needed)
echo "Waiting for Kafka brokers to start..."
sleep 15

# Create topics
echo "Creating topics..."
docker exec kafka1 kafka-topics --create \
  --topic raw_data \
  --bootstrap-server kafka1:9092 \
  --partitions 8 \
  --replication-factor 2

docker exec kafka1 kafka-topics --create \
  --topic processed_data \
  --bootstrap-server kafka1:9092 \
  --partitions 8 \
  --replication-factor 2

docker exec kafka1 kafka-topics --create \
  --topic data_quality \
  --bootstrap-server kafka1:9092 \
  --partitions 8 \
  --replication-factor 2

docker exec kafka1 kafka-topics --create \
  --topic predictions \
  --bootstrap-server kafka1:9092 \
  --partitions 8 \
  --replication-factor 2

echo "Topics created. Showing list:"
docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:9092
