#!/usr/bin/env bash

# Please put this file under ${PROJECT_HOME}/bin folder
# $>${PROJECT_HOME}/bin/run.sh [app.args]
# E.g. $>${PROJECT_HOME}/bin/run.sh [app.args]

if [ -L ${BASH_SOURCE-$0} ]; then
  FWDIR=$(dirname $(readlink "${BASH_SOURCE-$0}"))
else
  FWDIR=$(dirname "${BASH_SOURCE-$0}")
fi

if [[ -z "${PROJECT_HOME}" ]]; then
  export PROJECT_HOME="$(cd "${FWDIR}/.."; pwd)"
fi

if [[ -z "${PROJECT_NAME}" ]]; then
  export PROJECT_NAME=${PROJECT_HOME##*/}
fi
# conf
if [[ -z "${PROJECT_CONF_DIR}" ]]; then
  export PROJECT_CONF_DIR="$PROJECT_HOME/conf"
fi

# lib
if [[ -z "${PROJECT_LIB_DIR}" ]]; then
  export PROJECT_LIB_DIR="$PROJECT_HOME/lib"
fi

# bin
if [[ -z "${PROJECT_BIN_DIR}" ]]; then
  export PROJECT_BIN_DIR="$PROJECT_HOME/bin"
fi

if [[ -f "${PROJECT_CONF_DIR}/app-env.sh" ]]; then
  . "${PROJECT_CONF_DIR}/app-env.sh"
fi

if [[ -z "${SPARK_HOME}" || -z "${MAIN_CLASS}" ]]; then
  echo "[ERROR] SPARK_HOME or MAIN_CLASS is empty!"
  exit -1
fi

if [[ -z "${CLASS_PATH}" ]]; then
	CLASS_PATH="${PROJECT_LIB_DIR}:${PROJECT_CONF_DIR}"
else
	CLASS_PATH="${CLASS_PATH}:${PROJECT_LIB_DIR}:${PROJECT_CONF_DIR}"
fi

JOB_JAR=`ls ${PROJECT_HOME}/*${PROJECT_NAME}*.jar`
SPARK_SUBMIT_JARS="${JOB_JAR}"
if [[ -d "${PROJECT_CONF_DIR}" ]]; then
    for jar in $(find -L "${PROJECT_CONF_DIR}" -type f -name '*'); do
      if [[ -z $PROJECT_CONF_FILES ]]; then
        PROJECT_CONF_FILES="$jar"
      else
        PROJECT_CONF_FILES="$jar,${PROJECT_CONF_FILES}"
      fi
    done
fi

if [[ -d "${PROJECT_LIB_DIR}" ]]; then
    for jar in $(find -L "${PROJECT_LIB_DIR}" -type f -name '*jar'); do
      SPARK_SUBMIT_JARS="$jar,$SPARK_SUBMIT_JARS"
    done
fi
#--jars ${SPARK_SUBMIT_JARS}
SPARK_SUBMIT_ARGS="--class ${MAIN_CLASS} --jars ${SPARK_SUBMIT_JARS} --name ${PROJECT_NAME} --driver-library-path ${PROJECT_LIB_DIR} --driver-class-path ${CLASS_PATH}"

if [[ ! -z ${JAVA_OPTS} ]]; then
    SPARK_SUBMIT_ARGS="${SPARK_SUBMIT_ARGS} --driver-java-options ${JAVA_OPTS}"
fi

if [[ ! -z ${PROJECT_CONF_FILES} ]];then
    SPARK_SUBMIT_ARGS="${SPARK_SUBMIT_ARGS} --files ${PROJECT_CONF_FILES}"
fi

SPARK_SUBMIT_ARGS="${SPARK_SUBMIT_OPTS} ${SPARK_SUBMIT_ARGS}"

spark_submit="${SPARK_HOME}/bin/spark-submit"

if [[ -f "${spark_submit}" && -x "${spark_submit}" ]]; then
  # spark-submit [options] app.args
  exec "${spark_submit}" ${SPARK_SUBMIT_ARGS} "${JOB_JAR}" "$@"
else
  exit -1
fi

