# Spark + Hadoop Lab

## Dataset
Spotify Tracks Dataset (114k rows, 20 features)

## Experiments
1. 1 DataNode
2. 1 DataNode (optimized)
3. 3 DataNode
4. 3 DataNode (optimized)

## Optimizations
- repartition
- cache

## Results
(таблица + график)

## Conclusion
- 3 nodes faster than 1
- optimization reduces execution time


```sh
docker compose -f docker-compose-1node.yml up -d
# wait for 10-20 seconds before continue 
docker exec -it namenode bash

cd /data/
bash upload_to_hdfs.sh 

# verify file uploaded
hdfs dfs -ls /input                         # file existence verification
hdfs dfs -du -h /input                      # file size check
hdfs dfs -cat /input/spotify.csv | head     # first 10 file rows output


docker exec -it spark-master /opt/spark/bin/spark-submit /app/app.py

# docker exec -it spark-master \
#   /opt/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   /app/app.py


docker compose -f docker-compose-1node.yml down -v
```