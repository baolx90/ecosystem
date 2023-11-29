#!/bin/bash

docker build -t hadoop-base .

docker-compose -f docker-compose.yml up -d

# Run Spark Cluster
if [ "$PWD" != "spark" ]; then
  cd spark && ./start-cluster.sh && cd ..
fi

echo "Current dir is $PWD"
