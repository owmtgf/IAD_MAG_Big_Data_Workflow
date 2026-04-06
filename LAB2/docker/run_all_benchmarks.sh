#!/bin/bash
set -e

export RED='\033[1;31m'
export GREEN='\033[1;32m'
export YELLOW='\033[1;33m'
export NC='\033[0m'


run() {
  export NUM_DATA_NODES=$1
  export OPTIMIZED=$2
  echo -e "${GREEN}[INFO]${NC} Run experiment (DataNodes: $NUM_DATA_NODES; Optimized: $OPTIMIZED)"

  echo ""
  echo "======================================"
  echo "Running experiment:"
  echo "Nodes: $NUM_DATA_NODES"
  echo "Optimized: $OPTIMIZED"
  echo "======================================"

  ./run_benchmark.sh
  echo -e "${GREEN}[INFO]${NC} Waiting before next run (10s.)..."

  sleep 10
}

# (1 node, normal)
run 1 false

# (1 node, optimized)
run 1 true

# (3 nodes, normal)
run 3 false

# (3 nodes, optimized)
run 3 true

echo -e "${GREEN}[DONE]${NC} All experiments finished."