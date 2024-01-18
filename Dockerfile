# Alpine 3.11 contains Python 3.8, pyspark only supports Python up to 3.7

################################-- Start Packaging --######################################
FROM alpine:3.10.4 as env_package

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories

RUN apk add --no-cache \
    'curl=~7.66' \
    'unzip=~6.0' \
    'openjdk8=~8' \
    'bash=~5.0' \
    'coreutils=~8.31' \
    'procps=~3.3' \
    'findutils=~4.6' \
    'ncurses=~6.1' \
    'g++=~8.3' \
    'libc6-compat=~1.1' \
	tcl tk expect \
    && ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
	
VOLUME [ "/sys/fs/cgroup" ]

RUN apk update && \
    apk add openssh-server openssh-client openrc tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    rc-update add sshd && \
    mkdir -p /run/openrc && touch /run/openrc/softlevel && \
    openrc && \
    rc-status && \
    sed -i "s/#Port 22/Port 22/g" /etc/ssh/sshd_config && \
    sed -i "s/#PermitRootLogin.*/PermitRootLogin yes/g" /etc/ssh/sshd_config && \
    mkdir -p /root/.ssh && chmod 700 /root/.ssh/ && \
    ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa && \
    service sshd restart && \
    echo "root:${ROOT_PWD}" | chpasswd && \
    apk del tzdata && \
    rm -rf /var/cache/apk/* 
	

# PySpark - comment out if you don't want it in order to save image space
RUN apk add  --no-cache \
    'python3=~3.7' \
    'python3-dev=~3.7' \
    && ln -s /usr/bin/python3 /usr/bin/python \
	&& rm -rf /var/cache/apk/* 

COPY scripts/ /scripts
RUN apk add  'linux-headers=~4.19' \
   && gcc /scripts/watchdir.c -o /scripts/watchdir \
   && chmod +x /scripts/*

################################-- end Packaging --######################################

ENV USR_PROGRAM_DIR=/usr/program
ENV USR_BIN_DIR="${USR_PROGRAM_DIR}/source_dir"
RUN mkdir -p "${USR_BIN_DIR}"

# Common settings
ENV JAVA_HOME "/usr/lib/jvm/java-1.8-openjdk"
ENV PATH="${PATH}:${JAVA_HOME}/bin"
# http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed
ENV PYTHONHASHSEED 0
ENV PYTHONIOENCODING UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK 1

# Hadoop
ENV HADOOP_VERSION=3.2.3
ENV HADOOP_HOME /usr/program/hadoop
ENV HADOOP_NNAMENADE_HOSTNAME=master
ENV HADOOP_PACKAGE="hadoop-${HADOOP_VERSION}.tar.gz"
ENV PATH="${PATH}:${HADOOP_HOME}/bin"
ENV PATH="${PATH}:${HADOOP_HOME}/sbin"
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"
ENV LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}"
ENV HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
ENV HADOOP_LOG_DIR="${HADOOP_HOME}/logs"
# For S3 to work. Without this line you'll get "Class org.apache.hadoop.fs.s3a.S3AFileSystem not found" exception when accessing S3 from Hadoop
ENV HADOOP_CLASSPATH="${HADOOP_HOME}/share/hadoop/tools/lib/*"

# Hive
ENV HIVE_VERSION=3.1.2
ENV HIVE_HOME=/usr/program/hive
ENV HIVE_CONF_DIR="${HIVE_HOME}/conf"
ENV HIVE_LOG_DIR="${HIVE_HOME}/logs"
ENV HIVE_PACKAGE="apache-hive-${HIVE_VERSION}-bin.tar.gz"
ENV PATH="${PATH}:${HIVE_HOME}/bin"
ENV HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HIVE_HOME}/lib/*"
# Hive Hudi support
ENV HIVE_AUX_JARS_PATH=/usr/program/hive/lib/hudi-hadoop-mr-bundle-0.10.0.jar,/usr/program/hive/lib/hudi-hive-sync-bundle-0.10.0.jar

# HBase
ENV HBASE_VERSION=2.3.6
ENV HBASE_HOME=/usr/program/hbase
ENV HBASE_CONF_DIR="${HBASE_HOME}/conf/"
ENV HBASE_PACKAGE="hbase-${HBASE_VERSION}-bin.tar.gz"
ENV PATH="${PATH}:${HBASE_HOME}/bin"
ENV HBASE_LOG_DIR="${HBASE_HOME}/logs"

# Spark
ENV SPARK_VERSION=3.0.0
ENV SPARK_HADOOP_VERSION=3.2
ENV SPARK_HOME=/usr/program/spark
ENV SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz"
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV SPARK_CONF_DIR="${SPARK_HOME}/conf"
ENV SPARK_LOG_DIR="${SPARK_HOME}/logs"
ENV SPARK_DIST_CLASSPATH="${HADOOP_CONF_DIR}:${HADOOP_HOME}/share/hadoop/tools/lib/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/yarn:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*"

# Sqoop
ENV SQOOP_VERSION=1.4.7
ENV HADOOP_SQOOP_VERSION=2.6.0
ENV SQOOP_HOME=/usr/program/sqoop
ENV SQOOP_PACKAGE="sqoop-${SQOOP_VERSION}.bin__hadoop-${HADOOP_SQOOP_VERSION}.tar.gz"
ENV PATH="${PATH}:${SQOOP_HOME}/bin"
ENV HADOOP_COMMON_HOME="${HADOOP_HOME}"
ENV HADOOP_MAPRED_HOME="${HADOOP_HOME}"
ENV SQOOP_CONF_DIR="${SQOOP_HOME}/conf"
ENV SQOOP_LOG_DIR="${SQOOP_HOME}/logs"

# Tez
ENV TEZ_VERSION=0.9.2
ENV TEZ_HOME=/usr/program/tez
ENV TEZ_PACKAGE="apache-tez-${TEZ_VERSION}-bin.tar.gz"
ENV TEZ_CONF_DIR=${HADOOP_CONF_DIR}
ENV TEZ_JARS=${TEZ_HOME}/*:${TEZ_HOME}/lib/*
ENV HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}:${HADOOP_CLASSPATH}

# Hudi
ENV HUDI_VERSION=0.10.0

ENV ROOT_PWD=123456

COPY tar-source-files/* "${USR_PROGRAM_DIR}/source_dir/"
WORKDIR "${USR_PROGRAM_DIR}/source_dir"

# Hadoop Package
RUN if [ ! -f ${HADOOP_PACKAGE} ] ; then curl --progress-bar -L --retry 3 \
  "http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}" -o "${USR_PROGRAM_DIR}/source_dir/${HADOOP_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${HADOOP_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
  && mv "${USR_PROGRAM_DIR}/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
  && mkdir -p "${HADOOP_LOG_DIR}" \
  && rm -rf "${HADOOP_HOME}/share/doc" \
  && chown -R root:root "${HADOOP_HOME}"

RUN echo 'remove hadoop native dir' && rm -rf "${HADOOP_HOME}/lib/native"

# Hive Package
RUN if [ ! -f "${HIVE_PACKAGE}" ]; then curl --progress-bar -L \
   "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE}" -o "${USR_PROGRAM_DIR}/source_dir/${HIVE_PACKAGE}" ; fi \ 
   && tar -xf "${USR_PROGRAM_DIR}/source_dir/${HIVE_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
   && mv "${USR_PROGRAM_DIR}/apache-hive-${HIVE_VERSION}-bin" "${HIVE_HOME}" \
   && chown -R root:root "${HIVE_HOME}" \
   && mkdir -p "${HIVE_HOME}/hcatalog/var/log" \
   && mkdir -p "${HIVE_HOME}/var/log" \
   && mkdir -p "${HIVE_LOG_DIR}" \
   && chmod 755 "${HIVE_HOME}/hcatalog/var/log" \
   && chmod 755 "${HIVE_HOME}/var/log" \
   && chmod 755 "${HIVE_LOG_DIR}"

# HBase Package
RUN if [ ! -f "${HBASE_PACKAGE}" ]; then curl --progress-bar -L \
  "http://archive.apache.org/dist/hbase/${HBASE_VERSION}/${HBASE_PACKAGE}" -o "${USR_PROGRAM_DIR}/source_dir/${HBASE_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${HBASE_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
  && mv "${USR_PROGRAM_DIR}/hbase-${HBASE_VERSION}" "${HBASE_HOME}" \
  && chown -R root:root "${HBASE_HOME}"

# Spark Package
RUN if [ ! -f "${SPARK_PACKAGE}" ] ; then curl --progress-bar -L --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}" -o "${USR_PROGRAM_DIR}/source_dir/${SPARK_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${SPARK_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
  && mv "${USR_PROGRAM_DIR}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}" "${SPARK_HOME}" \
  && chown -R root:root "${SPARK_HOME}" 
  
# Sqoop Package
RUN if [ ! -f "${SQOOP_PACKAGE}" ]; then curl --progress-bar -L --retry 3 \
  "http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/${SQOOP_PACKAGE}" -o "${USR_PROGRAM_DIR}/source_dir/${SQOOP_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${SQOOP_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
  && mv "${USR_PROGRAM_DIR}/sqoop-${SQOOP_VERSION}.bin__hadoop-${HADOOP_SQOOP_VERSION}" "${SQOOP_HOME}" \
  && chown -R root:root "${SQOOP_HOME}" \
  && mkdir -p "${SQOOP_HOME}/logs"


# Tez Package
RUN if [ ! -f "${TEZ_PACKAGE}" ]; then curl --progress-bar -L --retry 3 \
  "https://dlcdn.apache.org/tez/${TEZ_VERSION}/apache-tez-${TEZ_VERSION}-bin.tar.gz" -o "${USR_PROGRAM_DIR}/source_dir/${TEZ_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${TEZ_PACKAGE}" -C "${USR_PROGRAM_DIR}/" \
  && mv "${USR_PROGRAM_DIR}/apache-tez-${TEZ_VERSION}-bin" "${TEZ_HOME}" \
  && chown -R root:root "${TEZ_HOME}" \
  && mkdir -p "${TEZ_HOME}/logs" 

# HUDI Package
RUN cp "${USR_PROGRAM_DIR}/source_dir/hudi-hive-sync-bundle-0.10.0.jar" "${HIVE_HOME}/lib/" \
 && cp "${USR_PROGRAM_DIR}/source_dir/hudi-hadoop-mr-bundle-0.10.0.jar" "${HIVE_HOME}/lib/" \
 && cp "${USR_PROGRAM_DIR}/source_dir/hudi-spark3-bundle_2.12-0.10.0.jar" "${SPARK_HOME}/jars/"

# Trino(PrestoSQL)
ENV TRINO_VERSION=378
ENV TRINO_HOME=/usr/program/trino-server
ENV TRINO_CONF_DIR=${TRINO_HOME}/etc
ENV ZULU_JDK11_HOME=/usr/program/zulu-jdk11

ENV ZULU_JDK11_PACKAGE=zulu11.56.19-ca-jdk11.0.15-linux_musl_x64.tar.gz
ENV ZULU_JDK11_UNPACK=zulu11.56.19-ca-jdk11.0.15-linux_musl_x64
RUN if [ ! -f "${ZULU_JDK11_PACKAGE}" ]; then curl --progress-bar -L --retry 3 \
  "https://cdn.azul.com/zulu/bin/${ZULU_JDK11_PACKAGE}"  -o "${USR_PROGRAM_DIR}/source_dir/${ZULU_JDK11_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${ZULU_JDK11_PACKAGE}" -C "${USR_PROGRAM_DIR}" \
  && mv "${USR_PROGRAM_DIR}/${ZULU_JDK11_UNPACK}" "${ZULU_JDK11_HOME}" \
  && chown -R root:root "${ZULU_JDK11_HOME}"

ENV TRINO_PACKAGE=trino-server-${TRINO_VERSION}.tar.gz
RUN if [ ! -f "${TRINO_PACKAGE}" ]; then curl --progress-bar -L --retry 3 \
  "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/${TRINO_PACKAGE}"  -o "${USR_PROGRAM_DIR}/source_dir/${TRINO_PACKAGE}" ; fi \
  && tar -xf "${USR_PROGRAM_DIR}/source_dir/${TRINO_PACKAGE}" -C "${USR_PROGRAM_DIR}" \
  && mv "${USR_PROGRAM_DIR}/trino-server-${TRINO_VERSION}" "${TRINO_HOME}" \
  && mkdir -p "${TRINO_HOME}/etc/catalog" \
  && chown -R root:root "${TRINO_HOME}" 

RUN rm -rf "${USR_PROGRAM_DIR}/source_dir/*" \
    && rm -rf "${HIVE_HOME}/examples" \
    && rm -rf "${SPARK_HOME}/examples/src" \
    && rm /usr/program/hive/lib/guava-19.0.jar \
    && cp  /usr/program/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /usr/program/hive/lib/ \
    && rm -rf "${USR_PROGRAM_DIR}/source_dir"
###############################################-- End Packaging --############################################

RUN mkdir -p "${USR_PROGRAM_DIR}/source_dir"

WORKDIR "${USR_PROGRAM_DIR}"

COPY requirements/requirements.txt .
RUN pip3 install -r requirements.txt

# Hadoop setup
COPY conf/hadoop/core-site.xml "${HADOOP_CONF_DIR}"/
COPY conf/hadoop/hadoop-env.sh "${HADOOP_CONF_DIR}"/
COPY conf/hadoop/hdfs-site.xml "${HADOOP_CONF_DIR}"/
COPY conf/hadoop/mapred-site.xml "${HADOOP_CONF_DIR}"/
COPY conf/hadoop/workers "${HADOOP_CONF_DIR}"/
COPY conf/hadoop/yarn-site.xml "${HADOOP_CONF_DIR}"/
COPY conf/tez/tez-site.xml "${HADOOP_CONF_DIR}"/

# Hive setup
COPY conf/hive/hive-site.xml "${HIVE_CONF_DIR}"/
COPY conf/hive/hive-env.sh "${HIVE_CONF_DIR}"/
# COPY conf/tez/tez-site.xml "${HIVE_CONF_DIR}"/
COPY conf/hive/hive-log4j2.properties "${HIVE_CONF_DIR}"/
COPY jdbc_drivers/* "${HIVE_HOME}/lib/"

RUN wget http://canali.web.cern.ch/res/hbase-spark-1.0.1-SNAPSHOT_spark331_hbase2415.jar
RUN mv hbase-spark-1.0.1-SNAPSHOT_spark331_hbase2415.jar "${HBASE_HOME}/lib/"

RUN wget http://canali.web.cern.ch/res/hbase-spark-protocol-shaded-1.0.1-SNAPSHOT_spark331_hbase2415.jar
RUN mv hbase-spark-protocol-shaded-1.0.1-SNAPSHOT_spark331_hbase2415.jar "${HBASE_HOME}/lib/"

# it is a pity than two scripts below would not work, then script for same goat have been added in entrypoint.sh file.
RUN for jar in `ls $TEZ_HOME | grep jar`; do export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_HOME/$jar; done 
RUN for jar in `ls $TEZ_HOME/lib`; do export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_HOME/lib/$jar; done

# Spark setup
COPY conf/hadoop/core-site.xml "${SPARK_CONF_DIR}"/
COPY conf/hadoop/hdfs-site.xml "${SPARK_CONF_DIR}"/
COPY conf/hadoop/yarn-site.xml "${SPARK_CONF_DIR}"/
COPY conf/hive/hive-site.xml "${SPARK_CONF_DIR}"/
COPY conf/spark/spark-defaults.conf "${SPARK_CONF_DIR}"/
COPY jdbc_drivers/* "${SPARK_HOME}/jars/"
COPY spark_jars/* "${SPARK_HOME}/jars/"

# HBase setup
COPY conf/hbase/hbase-env.sh "${HBASE_CONF_DIR}"
COPY conf/hbase/hbase-site.xml "${HBASE_CONF_DIR}"
COPY conf/hadoop/core-site.xml "${HBASE_CONF_DIR}"
COPY conf/hadoop/hdfs-site.xml "${HBASE_CONF_DIR}"
RUN echo "export JAVA_HOME=${JAVA_HOME}" >>  "${HBASE_CONF_DIR}/hbase-env.sh"

RUN wget http://canali.web.cern.ch/res/hbase-spark-1.0.1-SNAPSHOT_spark331_hbase2415.jar
RUN mv hbase-spark-1.0.1-SNAPSHOT_spark331_hbase2415.jar "${HBASE_HOME}/lib/"

RUN wget http://canali.web.cern.ch/res/hbase-spark-protocol-shaded-1.0.1-SNAPSHOT_spark331_hbase2415.jar
RUN mv hbase-spark-protocol-shaded-1.0.1-SNAPSHOT_spark331_hbase2415.jar "${HBASE_HOME}/lib/"

# Sqoop setup
COPY jdbc_drivers/* "${SQOOP_HOME}/lib"/
COPY conf/sqoop/* "${SQOOP_CONF_DIR}"/
COPY conf/hive/hive-site.xml "${SQOOP_CONF_DIR}"/
RUN cp "${HIVE_HOME}/lib/hive-exec-${HIVE_VERSION}.jar" "${SQOOP_HOME}/lib"/

# If both YARN Web UI and Spark UI is up, then returns 0, 1 otherwise.
HEALTHCHECK CMD curl -f http://host.docker.internal:8080/ \
    && curl -f http://host.docker.internal:8088/ || exit 1

# Entry point: start all services and applications.
COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

WORKDIR /usr/program
ENTRYPOINT ["/entrypoint.sh"]
