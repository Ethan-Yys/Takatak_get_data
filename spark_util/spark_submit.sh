#!/bin/bash


TODAY=$(date +%Y%m%d -u)
RM_LOG_DATE=$(date +%Y%m%d -d @$[ $(date +%s -d"${TODAY}") - (86400 * 3) ])

WORK_PATH=$(cd $(dirname "$0"); pwd -P)
TOOL_PATH=${WORK_PATH%/MXMachineLearning*}/MXMachineLearning/tools
LOG_PATH=${WORK_PATH}/log


AWS=/usr/bin/aws
PYTHON=/usr/bin/python
SPARK=/usr/bin/spark-submit


JOB_PREFIX=$1
TIMEOUT=$2

DRIVER_MEM=$3
EXE_NUM=$4
EXE_CORES=$5
EXE_MEM=$6
EXE_MAX=$7
SHUFFLE_NUM=$8

SCRIPT=$9
PARAMS=${10}
PY_FILES=${11}


LOG_PREFIX=${LOG_PATH}/log.spark.cluster.${SCRIPT%.*}
LOG_FILE=${LOG_PREFIX}.${TODAY}
RM_LOG_FILE=${LOG_PREFIX}.${RM_LOG_DATE}


function run_spark_cluster(){

    #Usage: run_cluster_spark \
    #        job_prefix \
    #        timeout \
    #        driver_memory \
    #        executor_num \
    #        executor_cores \
    #        executor_memory \
    #        executor_max \
    #        shuffle_num \
    #        script \
    #        params \
    #        py_files

    job_prefix=$1
	timeout=$2

    driver_memory=$3
    executor_num=$4
    executor_cores=$5
    executor_memory=$6
    executor_max=$7
    shuffle_num=$8

    script=$9
    params=${10}
    py_files=${11}

    timeout ${timeout} ${SPARK} \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory ${driver_memory} \
        --num-executors ${executor_num} \
        --executor-cores ${executor_cores} \
        --executor-memory ${executor_memory} \
        --conf spark.dynamicAllocation.maxExecutors=${executor_max} \
        --conf spark.sql.shuffle.partitions=${shuffle_num} \
        --conf spark.default.parallelism=${shuffle_num} \
        --conf spark.driver.maxResultSize=${driver_memory} \
        --conf spark.yarn.maxAppAttempts=1 \
        --conf spark.hadoop.validateOutputSpecs="false" \
        --conf spark.ui.showConsoleProgress=true \
        --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
        --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
        --conf spark.yarn.nodemanager.localizer.cache.target-size-mb=4g \
        --conf spark.yarn.nodemanager.localizer.cache.cleanup.interval-ms=300000 \
        --files "${WORK_PATH}/log4j.properties" \
        --py-files ${py_files} \
        --name "${job_prefix} => ${script} ${params}" \
        ${script} ${params}
}


function build_dir(){
    dir=$1
    if [ ! -d ${dir} ]; then
        mkdir -p ${dir}
    fi
}

function rm_file(){
    file=$1
    if [ -f ${file} ]; then
        rm ${file}
    fi
}



build_dir ${LOG_PATH}
rm_file ${RM_LOG_FILE}


echo ${LOG_FILE}

run_spark_cluster \
    "${JOB_PREFIX}" \
    ${TIMEOUT} \
    ${DRIVER_MEM} \
    ${EXE_NUM} \
    ${EXE_CORES} \
    ${EXE_MEM} \
    ${EXE_MAX} \
    ${SHUFFLE_NUM} \
    ${SCRIPT} \
    "${PARAMS}" \
    "${PY_FILES}" >${LOG_FILE} 2>&1
#bash ${TOOL_PATH}/check_spark.sh $? ${LOG_FILE} "${SCRIPT} ${PARAMS}" "${JOB_PREFIX}"
exit_status=$?

cd ${WORK_PATH}

exit ${exit_status}

