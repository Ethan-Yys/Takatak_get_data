#!/bin/bash
# 9 9 * * * nohup /mnt1/researcher/workspace/kikyoussionild_session_movie.sh > /mnt1/researcher/workspace/kikyoussion/bms_log 2>&1 &
AWS="/usr/bin/aws"
PYTHON="/usr/bin/python"
SPARK="/usr/bin/spark-submit"

session_duration=3600
# main
$SPARK \
--name daily_active_user_num \
--master yarn  --deploy-mode cluster \
--executor-memory 4g \
--executor-cores 2 \
--num-executors 200 \
--driver-memory 4g \
--py-files ../../util/datetime_tools.py \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.default.parallelism=400 \
--conf spark.yarn.executor.memoryOverhead=4G \
--conf spark.yarn.nodemanager.localizer.cache.target-size-mb=4g \
--conf spark.yarn.nodemanager.localizer.cache.cleanup.interval-ms=300000 \
--files s3://mx-machine-learning/luomingjun/others/spark/log4j.properties \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
--conf spark.dynamicAllocation.maxExecutors=200 \
how_many_user.py $1 $2