#!/usr/bin/bash


AWS=/usr/bin/aws

WORK_PATH=$(cd $(dirname "$0"); pwd -P)
TOOL_PATH=${WORK_PATH%/MXMachineLearning*}/MXMachineLearning/tools


function wait_s3_data(){

    if [ $# -eq 3 ]; then
        s3_path=$1
        wait_times=$2
        time_wait=$3
    elif [ $# -eq 2 ]; then
        s3_path=$1
        wait_times=$2
        time_wait=600
    else
        echo "Usage: wait_s3_data [s3_path] [wait_times] | [time_wait=600]"
        return 2
    fi

    for try_time in $(seq 1 ${wait_times}); do

        ${AWS} s3 ls ${s3_path}/_SUCCESS
        if [ $? -eq 0 ]; then
            return 0
        else
            echo "$(date) INFO => wait ${s3_path}: try ${try_time} * ${time_wait}s"
            sleep ${time_wait}
        fi

    done

    return 1

}


function wait_s3_file(){

    if [ $# -eq 3 ]; then
        s3_file=$1
        wait_times=$2
        time_wait=$3
    elif [ $# -eq 2 ]; then
        s3_file=$1
        wait_times=$2
        time_wait=600
    else
        echo "Usage: wait_s3_data [s3_file] [wait_times] | [time_wait=600]"
        return 2
    fi

    for try_time in $(seq 1 ${wait_times}); do

        ${AWS} s3 ls ${s3_file}
        if [ $? -eq 0 ]; then
            return 0
        else
            echo "$(date) INFO => wait ${s3_file}: try ${try_time} * ${time_wait}s"
            sleep ${time_wait}
        fi

    done

    return 1

}
