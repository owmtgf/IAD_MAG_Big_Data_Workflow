#!/bin/bash
set -e

NUM_DATA_NODES=${NUM_DATA_NODES:-1}
OPTIMIZED=${OPTIMIZED:-false}

echo -e "${GREEN}[INFO]${NC} Starting cluster with $NUM_DATA_NODES datanodes..."
echo -e "${GREEN}[INFO]${NC} Optimized: $OPTIMIZED"
docker compose -f docker-compose.yml up -d --build --scale datanode=$NUM_DATA_NODES

echo -e "${YELLOW}[INFO]${NC} Waiting for HDFS..."
until docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; do
  echo "Waiting for HDFS..."
  sleep 5
done
echo -e "${GREEN}[INFO]${NC} HDFS is ready."

echo -e "${YELLOW}[INFO]${NC} Uploading data to HDFS..."
docker exec namenode chmod +x /data/upload_to_hdfs.sh
docker exec namenode /data/upload_to_hdfs.sh
echo -e "${GREEN}[INFO]${NC} Done uploading to HDFS"

echo -e "${YELLOW}[INFO]${NC} Running Spark job..."

if [ "$OPTIMIZED" = true ]; then
  OPT_FLAG="--optimized"
else
  OPT_FLAG=""
fi

docker exec spark-master mkdir -p /logs
docker exec spark-master chmod -R a+rwx /logs

docker exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --executor-cores 2 \
  /app/app.py \
    --nodes "$NUM_DATA_NODES" \
    $OPT_FLAG

echo -e "${GREEN}[INFO]${NC} Job done."
echo -e "${GREEN}[INFO]${NC} Saving logs..."
mkdir -p ../results/logs
docker compose logs > ../results/logs/run_${NUM_DATA_NODES}_${OPTIMIZED}.log

echo -e "${GREEN}[INFO]${NC} Stopping cluster..."
docker compose -f docker-compose.yml down -v