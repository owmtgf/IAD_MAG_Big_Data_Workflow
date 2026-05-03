#!/bin/bash
set -e

mkdir -p logs

echo "Starting docker compose..."
docker compose up -d --build mlflow

docker compose ps
docker compose logs --tail=50 mlflow

echo "Waiting for MLflow on localhost:5000..."
until curl -f -s http://localhost:5000 > /dev/null; do
  docker compose ps mlflow
  sleep 2
done

echo "MLflow is ready"

echo "Running app pipeline..."
docker compose build --no-cache app
docker compose run --rm app

echo "Pipeline finished"
echo "MLflow UI: http://localhost:5000"
