
docker compose up --build

docker exec -it lab1-kafka-1 kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 1

  docker exec -it lab1-kafka-1 kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic topic-2 \
  --partitions 2 \
  --replication-factor 1