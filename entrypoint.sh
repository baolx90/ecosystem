#!/bin/bash

if [ -n "${HADOOP_DATANODE_UI_PORT}" ]; then
  echo "Replacing default datanode UI port 9864 with ${HADOOP_DATANODE_UI_PORT}"
  sed -i "$ i\<property><name>dfs.datanode.http.address</name><value>0.0.0.0:${HADOOP_DATANODE_UI_PORT}</value></property>" ${HADOOP_CONF_DIR}/hdfs-site.xml
fi
if [ "${HADOOP_NODE}" == "namenode" ]; then
  echo "Starting Hadoop name node..."
  hdfs namenode -format
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager
  mapred --daemon start historyserver
fi
if [ "${HADOOP_NODE}" == "datanode" ]; then
  echo "Starting Hadoop data node..."
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
fi

if [ -n "${HIVE_CONFIGURE}" ]; then
  echo "Configuring Hive..."
  schematool -dbType postgres -initSchema

  # Start metastore service.
  hive --service metastore &

  # JDBC Server.
  hiveserver2 &
fi

if [ -z "${SPARK_MASTER_ADDRESS}" ]; then
  echo "Starting Spark master node..."
  # Create directory for Spark logs
  SPARK_LOGS_HDFS_PATH=/log/spark
  if ! hadoop fs -test -d "${SPARK_LOGS_HDFS_PATH}"
  then
    hadoop fs -mkdir -p  ${SPARK_LOGS_HDFS_PATH}
    hadoop fs -chmod -R 755 ${SPARK_LOGS_HDFS_PATH}/*
  fi

  hdfs dfs -mkdir -p /hbase
  hdfs dfs -chmod -R 755 /hbase

  hdfs dfs -mkdir -p /hive
  hdfs dfs -chmod -R 755 /hive

  # Spark on YARN
  # SPARK_JARS_HDFS_PATH=/spark-jars
  # if ! hadoop fs -test -d "${SPARK_JARS_HDFS_PATH}"
  # then
  #   hadoop dfs -copyFromLocal "${SPARK_HOME}/jars" "${SPARK_JARS_HDFS_PATH}"
  # fi

  "${SPARK_HOME}/sbin/start-master.sh" -h master &
  "${SPARK_HOME}/sbin/start-history-server.sh" &
else
  echo "Starting Spark slave node..."
  "${SPARK_HOME}/sbin/start-slave.sh" "${SPARK_MASTER_ADDRESS}" &
fi

if [ -n "${HBASE_ROLE}" ]; then
  echo "`date` Starting HBASE" 
  for role in ${HBASE_ROLE}; do
    echo "`date` Starting HBASE - ${role}" 
    if [ "${role}" = "hmaster" ]; then
        echo "`date` Starting hmaster main on `hostname`" 
        hbase-daemon.sh start master || echo "error: start hmaster fail on `hostname`"
    fi

    if [ "${role}" = "regionserver" ]; then
        echo "`date` Starting regionserver on `hostname`" 
        hbase-daemon.sh start regionserver || echo "error: start regionserver fail on `hostname`"
    fi
    if [ "${role}" = "thrift" ]; then
        echo "`date` Starting thrift on `hostname`" 
        hbase-daemon.sh start thrift2 || echo "error: start thrift2 fail on `hostname`"
    fi
    if [ "${role}" = "hmaster_backup" ]; then
        echo "`date` Starting hmaster backup on `hostname`" 
        hbase-daemon.sh start master --backup || echo "error: start hmaster-backup fail on `hostname`"
    fi
  done
fi

echo "All initializations finished!"

tail -f /dev/null