#!/usr/bin/env bash

SPARK_HOME="/usr/hdp/2.5.3.0-37/spark"
SPARK_MASTER="yarn"
MAIN_CLASS="com.hand.ueba.streaming.Launcher.Launcher"
SPARK_SUBMIT_OPTS="--deploy-mode client --driver-memory 2g --executor-cores 4 --executor-memory 4g --num-executors 5"
#HADOOP_CONF_DIR=
#JAVA_HOME=
#CLASS_PATH=
#JAVA_OPTS=
