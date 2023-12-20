#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then
  hdfs namenode -format

  # start the master node processes
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # create required directories
  while ! hdfs dfs -mkdir -p /spark_logs;
  do
    echo "Failed creating /spark_logs hdfs dir"
  done
  echo "Created /spark_logs hdfs dir"

  hdfs dfs -mkdir -p /spark_data
  echo "Created /spark_data hdfs dir"


  # copy the data to the data HDFS directory
  hdfs dfs -copyFromLocal /opt/spark/data/* /spark_data
  hdfs dfs -ls /spark_data

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  hdfs namenode -format

  # start the worker node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /spark_logs;
  do
    echo "spark_logs doesn't exist yet... retrying"
    sleep 1;
  done
  echo "Exit loop"

  # start the spark history server
  start-history-server.sh
fi

tail -f /dev/null
