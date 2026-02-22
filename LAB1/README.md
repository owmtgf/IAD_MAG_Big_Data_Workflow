# Kafka Producer-Consumer Pipeline

This template demonstrates a simple **Kafka producer-consumer workflow** using Docker Compose. It includes:

- **Producer**: generates random data and sends to Kafka topics.
- **Consumer**: reads data from Kafka topics and prints it.
- **Kafka + Zookeeper**: containerized message broker.

---

## Project Structure
```
.
├── consumer
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
├── docker-compose.yml
├── producer
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
└── run_containers.sh
```


- `consumer/` → Kafka consumer service
- `producer/` → Kafka producer service
- `docker-compose.yml` → orchestrates all services (Kafka, Zookeeper, producer, consumer)
- `run_containers.sh` → helper script to start Kafka topics & containers

---

## Prerequisites

- Docker  
- Docker Compose  
- Linux / WSL recommended for development  

---

## How to run

1. **Build and start all containers**:

```sh
bash run_containers.sh
```

or manually:

```
docker compose up --build
```

> Manual topics creation is optional.

2. Verify running containers:

```sh
docker compose ps
```

3. Check logs (producer/consumer workflow)

```sh
docker compose logs -f producer
docker compose logs -f consumer
```

## Stop and remove containers

```sh
docker compose down
```

## Notes

- Producer and consumer automatically connect to Kafka via the Docker network (`bootstrap_servers='kafka:9092'`).
- `PYTHONUNBUFFERED=1` ensures log messages are flushed immediately.
- Kafka topics are created in `run_containers.sh` with partitions and replication factor.