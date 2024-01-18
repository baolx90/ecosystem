#!/bin/bash

/etc/init.d/sshd restart

sed -i '/};/i permission javax.management.MBeanTrustPermission "register";' ${JAVA_HOME}/jre/lib/security/java.policy

sed -i 's/Warning: $HCAT_HOME does not exist! HCatalog jobs will fail./$HCAT_HOME does not exist!/g' ${SQOOP_HOME}/bin/configure-sqoop
sed -i 's/Warning: $ACCUMULO_HOME does not exist! Accumulo imports will fail./$ACCUMULO_HOME does not exist!/g' ${SQOOP_HOME}/bin/configure-sqoop


auto_ping(){
  if ping -c 1 -w 1 $1 > /dev/null;then
    echo "ping $1 success"
	return 0
  else
    echo "ping $1 fail"
	return -1
  fi
}

wait_until() {
    local hostname=${1?}
    local port=${2?}
    local retry=${3:-100}
    local sleep_secs=${4:-2}
    local address_up=0
    while [ ${retry} -gt 0 ] ; do
        echo  "Waiting until ${hostname}:${port} is up ... with remaining retry times: ${retry}"
        if nc -z ${hostname} ${port}; then
            address_up=1
            break
        fi
        retry=$((retry-1))
        sleep ${sleep_secs}
    done
    if [ $address_up -eq 0 ]; then
        echo "GIVE UP waiting until ${hostname}:${port} is up! "
        return 0
    else
     return 1
	fi
}

auto_ssh(){
username=$1
password=$2
hostname=$3
/usr/bin/expect <<EOF
set timeout 10
spawn ssh-copy-id -i /root/.ssh/id_rsa.pub $username@$hostname
expect {
            #first connect, no public key in ~/.ssh/known_hosts
            "*yes/no*" {
            send "yes\r"
            expect "*password*"
                send "$password\r"
            }
            #already has public key in ~/.ssh/known_hosts
            "*password*" {
                send "$password\r"
            }
            "Now try logging into the machine" {
                #it has authorized, do nothing!
            }
        }
expect eof
EOF
}

ssh_service(){
  ROOT_PWD=123456
  ssh_retry=30
  ssh_sleep_secs=10
  for hostname in ${HADOOP_SERVERS_HOSTNAME} ; do
    while [ ${ssh_retry} -gt 0 ] ; do
      auto_ping $hostname
  	res=$?
      if [ $res -eq 0 ] ; then
        echo "enter sending id_rsa loop,now hostname is $hostname"
        auto_ssh root ${ROOT_PWD} $hostname
        break
  	else
  	  sleep $ssh_sleep_secs
  	  continue
  	fi
    done
  done
}

HADOOP_CLASSPATH_FUNC=`$HADOOP_HOME/bin/hadoop classpath`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_CLASSPATH_FUNC


if [ -n "${HADOOP_DATANODE_UI_PORT}" ]; then
  echo "Replacing default datanode UI port 9864 with ${HADOOP_DATANODE_UI_PORT}"
  if [ -z "`grep "dfs.datanode.http.address" ${HADOOP_CONF_DIR}/hdfs-site.xml`" ]; then
    p="<property><name>dfs.datanode.http.address</name><value>0.0.0.0:${HADOOP_DATANODE_UI_PORT}</value></property>"
    sed -i "$ i\  $p  " ${HADOOP_CONF_DIR}/hdfs-site.xml
  fi
fi

CONTAINER_ALREADY_STARTED=/CONTAINER_ALREADY_STARTED_PLACEHOLDER
if [ ! -e $CONTAINER_ALREADY_STARTED ]; then
  ssh_service
  if [ "${HADOOP_NODE}" = "namenode" ]; then
    echo "-- container first startup : format namenade--"
  	hdfs namenode -format
	echo " container first startup : Configuring Hive..."
    schematool -dbType postgres -initSchema
    echo "-- container first startup : init trino configuration--"
    mv "${TRINO_CONF_DIR}/config.properties.coordinator" "${TRINO_CONF_DIR}/config.properties"   
  elif [ "${HADOOP_NODE}" = "datanode" ]; then 
    mv "${TRINO_CONF_DIR}/config.properties.worker" "${TRINO_CONF_DIR}/config.properties" 
  fi

  sed -i "s/node.id=.*/node.id=$RANDOM/g" "${TRINO_CONF_DIR}/node.properties"
else
  echo "-- Not container first startup --"
fi

if [ "${HADOOP_NODE}" = "namenode" ]; then
  echo "Starting Hadoop name node..."
  hdfs --daemon start namenode  || echo "error: start namenode fail"
  hdfs --daemon start secondarynamenode || echo "error: start secondarynamenode fail on `hostname`"
  yarn --daemon start resourcemanager  || echo "error: start resourcemanager fail on `hostname`"
  mapred --daemon start historyserver  || echo "error: start historyserver fail on `hostname`"
  # Start metastore service.
  hive --service metastore &
  # JDBC Server.
  hiveserver2 &
elif [ "${HADOOP_NODE}" = "datanode" ]; then
  echo "Starting Hadoop data node..."
  hdfs --daemon start datanode  || echo "error: start datanode fail on `hostname`"
  yarn --daemon start nodemanager  || echo "error: start nodemanager fail on `hostname`"
fi

hadoop_unalive=1
while [ $hadoop_unalive -ne 0 ] ; do
  hadoop fs -test -d /
  hadoop_unalive=$?
  sleep 5
  echo 'reach hdfs  fail,retrying !??'
done
echo 'reach hdfs success !!!'

if [ ! -e $CONTAINER_ALREADY_STARTED ]; then
  # first statup with hdfs ready
  if [ "${HADOOP_NODE}" = "namenode" ]; then
	echo "-- container first startup : init hbase dirs configuration--"
	hdfs dfs -mkdir -p /tmp /user/hdfs /hbase
    hdfs dfs -chmod 755 /tmp /user/hdfs /hbase

    echo '' > "${HBASE_CONF_DIR}/regionservers"
    for hostname in ${HBASE_REGIONSERVER_HOSTNAME}; do
      echo "$hostname"  >> $HBASE_CONF_DIR/regionservers
    done

    SPARK_LOGS_HDFS_PATH=/log/spark
    SPARK_JARS_HDFS_PATH=/spark-jars
    hadoop fs -mkdir -p  $SPARK_LOGS_HDFS_PATH $SPARK_JARS_HDFS_PATH
    hadoop fs -chmod -R 755 $SPARK_LOGS_HDFS_PATH $SPARK_JARS_HDFS_PATH
    # Spark on YARN
    hdfs dfs -put $SPARK_HOME/jars/* $SPARK_JARS_HDFS_PATH/
    # Tez JAR
    TEZ_JARS_HDFS_PATH=/tez
    hadoop dfs -mkdir -p $TEZ_JARS_HDFS_PATH
    hadoop dfs -put $TEZ_HOME/share/tez.tar.gz $TEZ_JARS_HDFS_PATH
    fi
 fi


if [ -z "${SPARK_MASTER_ADDRESS}" ]; then
  echo "Starting Spark master node..."
  "${SPARK_HOME}/sbin/start-master.sh" -h master &
  "${SPARK_HOME}/sbin/start-history-server.sh" &
else
  echo "Starting Spark slave node..."
  "${SPARK_HOME}/sbin/start-slave.sh" "${SPARK_MASTER_ADDRESS}" &
fi

for jar in `ls $TEZ_HOME |grep jar`; do
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_HOME/$jar
done
for jar in `ls $TEZ_HOME/lib`; do
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_HOME/lib/$jar
done


zoo_alive=0
for zoo_hostname in ${ZOO_SERVERS_HOSTNAME};do
  echo "try nc $zoo_hostname"
  wait_until ${zoo_hostname} 2181 1000000 15
  res=$?
  zoo_alive=$((zoo_alive+res))
  if [ $zoo_alive -gt 0 ] ;then
    echo "zookeeper port can be connected!"
    break
  fi
done

if [ $zoo_alive -gt 0 ] ; then
  echo "zookeeper is alive ready to start hbase"
  for role in ${HBASE_ROLE}; do
    # HBase master startup
    if [ "${role}" = "hmaster" ]; then
        echo "`date` Starting hmaster main on `hostname`" 
        hbase-daemon.sh start master || echo "error: start hmaster fail on `hostname`"
    fi
    # HBase regionserver startup
    if [ "${role}" = "regionserver" ]; then
        wait_until ${HBASE_MASTER_HOSTNAME} 16000 
        echo "`date` Starting regionserver on `hostname`" 
        hbase-daemon.sh start regionserver || echo "error: start regionserver fail on `hostname`"
    fi
    if [ "${role}" = "thrift" ]; then
	    # 对于regionserver 只取最左边第一个hostname
        wait_until ${HBASE_REGIONSERVER_HOSTNAME%% *} 16020
        echo "`date` Starting thrift on `hostname`" 
        hbase-daemon.sh start thrift || echo "error: start thrift2 fail on `hostname`"
    fi
	if [ "${role}" = "hmaster_backup" ]; then
        echo "`date` Starting hmaster backup on `hostname`" 
        hbase-daemon.sh start master --backup || echo "error: start hmaster-backup fail on `hostname`"
    fi
  done
else
  echo "zookeeper is not alive, start hbase cluster fail"
fi

echo "All initializations finished!"

touch $CONTAINER_ALREADY_STARTED

/scripts/parallel_commands.sh "/scripts/watchdir ${HADOOP_LOG_DIR}" "/scripts/watchdir ${SPARK_LOG_DIR}" "/scripts/watchdir ${HBASE_LOG_DIR}"

function stop_server() {
  echo "container terminating,ready to stop all bigdata service"

  for role in ${HBASE_ROLE}; do
    if [ "${role}" = "hmaster" ]; then
        hbase-daemon.sh stop master || echo "error: stop hmaster fail on `hostname`"
    fi
    if [ "${role}" = "regionserver" ]; then
        hbase-daemon.sh stop regionserver || echo "error: stop regionserver fail on `hostname`"
    fi
    if [ "${role}" = "thrift" ]; then
        hbase-deamon.sh stop thrift2 || echo "error: stop thrift2 fail on `hostname`"
    fi
    if [ "${role}" = "hmaster_backup" ]; then
        hbase-daemon.sh stop master --backup || echo "error: stop hmaster-backup fail on `hostname`"
    fi
  done

  if [ "${HADOOP_NODE}" = "namenode" ]; then
    hdfs namenode -format
    hdfs --daemon stop namenode
    hdfs --daemon stop secondarynamenode
    yarn --daemon stop resourcemanager
    mapred --daemon stop historyserver
  fi
  if [ "${HADOOP_NODE}" = "datanode" ]; then
      hdfs --daemon stop datanode
      yarn --daemon stop nodemanager
  fi
}


trap 'stop_server' SIGTERM          