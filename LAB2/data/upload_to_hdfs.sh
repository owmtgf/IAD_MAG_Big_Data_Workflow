#!/bin/bash

hdfs dfs -mkdir -p /input
hdfs dfs -put /data/spotify_tracks_clean.parquet /input/