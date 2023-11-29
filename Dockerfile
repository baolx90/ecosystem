FROM openjdk:11-jdk AS jdk

FROM python:3.11

USER root

# --------------------------------------------------------
# JAVA
# --------------------------------------------------------
RUN apt update
RUN apt-get install -y --no-install-recommends \
    python3-launchpadlib \
    software-properties-common
# RUN add-apt-repository ppa:openjdk-r/ppa
# RUN apt update
# RUN apt install -y --no-install-recommends \
#     openjdk-8-jdk
# For AMD based architecture use
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
COPY --from=jdk /usr/local/openjdk-11 /usr/lib/jvm/java-11-openjdk-arm64/
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64/

# --------------------------------------------------------
# HADOOP
# --------------------------------------------------------
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV PATH $HADOOP_PREFIX/bin/:$PATH
ENV PATH $HADOOP_HOME/bin/:$PATH

RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz*

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir /hadoop-data

USER root

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

COPY conf/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY conf/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY conf/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml

# ADD hadoop.env /hadoop.env

# RUN set -x && cd / && ./entrypoint.sh

# --------------------------------------------------------
# SPARK
# --------------------------------------------------------

ENV SPARK_VERSION=3.5.0 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

ADD conf/core-site.xml $SPARK_HOME/conf
ADD conf/yarn-site.xml $SPARK_HOME/conf

#=========
# INSTALL PYTHON DEPS
#=========
RUN apt-get update && add-apt-repository ppa:deadsnakes/ppa \
  && apt-get install -y --no-install-recommends \
         gcc \
         g++ \
         subversion \
         python3-dev \
         gfortran \
         build-essential \
         libopenblas-dev \
         liblapack-dev \
         libqpdf-dev \
         pkg-config \
         libzbar-dev \
         python3-dev \
         libpython3-dev \
         qpdf \
         xvfb \
         gconf-service \
         libasound2 \
         libatk1.0-0 \
         libcairo2 \
         libcups2 \
         libfontconfig1 \
         libgdk-pixbuf2.0-0 \
         libgtk-3-0 \
         libnspr4 \
         libpango-1.0-0 \
         libxss1 \
         fonts-liberation \
         libappindicator1 \
         libnss3 \
         lsb-release \
         xdg-utils \
         wget \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --default-timeout=100 --upgrade pip
RUN pip install pikepdf Cython numpy wheel setuptools --force-reinstall

#ADD requirements.txt /requirements.txt

# run install
#RUN pip install -r /requirements.txt
