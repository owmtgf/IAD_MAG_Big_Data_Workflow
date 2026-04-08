import json


def init_metrics(args):
    return {
        "config": {
            "optimized": args.optimized,
            "nodes": args.nodes
        },
        "timings": {},
        "memory": [],
        "partitions": None,
        "run_stats": {}
    }


def save_metrics(metrics, filename):
    with open(filename, "w") as f:
        json.dump(metrics, f, indent=4)
