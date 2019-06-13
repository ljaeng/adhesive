#!/bin/bash

if [ $# != 5 ];then
    echo "Please specify arguments: -config <config.file> -serverMode -name <appName>"
    echo "    example: ./sql_job.sh -config /workspace/project/sql_job/conf/ODS_DEVICE_ACTIVATION.json -serverMode -name sql-job-gyj-1"
    echo "    appName, required : please specify <AppName> for whatever you want"
    echo "    config.file, required: json config file"

    echo ""
    echo "Also, you can specify executor ** resources ** : "
    echo "    example: EXECUTOR_CPU=2;EXECUTOR_MEM=2;EXECUTOR_NUM=20; ./sql_job.sh -config /workspace/project/sql_job/conf/ODS_DEVICE_ACTIVATION.json -serverMode -name sql-job-gyj-1"
    echo "    EXECUTOR_CPU, optional: single executor cpu core number, default value = 2"
    echo "    EXECUTOR_MEM, optional: single executor memory in GB, default value = 2"
    echo "    EXECUTOR_NUM, optional: executor instance number, default value = 20"
    exit -1
fi

# ---
EXECUTOR_CPU=${EXECUTOR_CPU:-2}
EXECUTOR_MEM=${EXECUTOR_MEM:-2}
EXECUTOR_NUM=${EXECUTOR_NUM:-20}


total_cpu=$((${EXECUTOR_CPU} * ${EXECUTOR_NUM}))
total_mem=$((${EXECUTOR_MEM} * ${EXECUTOR_NUM}))
echo "Total Resources: CPU: ${total_cpu} MEM: ${total_mem}"
echo "Resources: EXECUTOR_CPU(${EXECUTOR_CPU}), EXECUTOR_MEM(${EXECUTOR_MEM}), EXECUTOR_NUM(${EXECUTOR_NUM})"



SPARK_HOME=/workspace/service/spark-2.1.0-bin-hadoop2.7

LIB_DIR=/workspace/project/sql_job/lib/
LIBS=$LIB_DIR/phoenix-spark-4.12.0-HBase-1.2.jar,$LIB_DIR/httpclient-4.3.6.jar,$LIB_DIR/tephra-hbase-compat-1.1-0.13.0-incubating.jar,$LIB_DIR/tephra-core-0.13.0-incubating.jar,$LIB_DIR/twill-discovery-api-0.8.0.jar,$LIB_DIR/tephra-api-0.13.0-incubating.jar,$LIB_DIR/twill-zookeeper-0.8.0.jar,$LIB_DIR/hbase-client-1.2.5.jar,$LIB_DIR/hbase-protocol-1.2.5.jar,$LIB_DIR/hbase-server-1.2.5.jar,$LIB_DIR/hbase-common-1.2.5.jar,$LIB_DIR/phoenix-core-4.12.0-HBase-1.2.jar,$LIB_DIR/kafka-clients-0.10.0.1.jar,$LIB_DIR/metrics-core-2.2.0.jar,$LIB_DIR/kafka_2.11-0.10.0.1.jar,$LIB_DIR/json-20090211.jar,$LIB_DIR/spark-streaming-kafka-0-10_2.11-2.1.0.jar,$LIB_DIR/fastjson-1.2.6.jar,$LIB_DIR/fastutil-6.5.7.jar,$LIB_DIR/mongo-java-driver-3.4.2.jar,$LIB_DIR/mongo-spark-connector_2.11-2.1.0.jar

CONF_DIR=/workspace/project/sql_job/conf/

$SPARK_HOME/bin/spark-submit \
    --name "sql_job.${APP_NAME}" \
    --master local \
    --conf spark.yarn.am.memory=1G \
    --conf spark.yarn.am.cores=2 \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --conf spark.port.maxRetries=100 \
    --conf spark.akka.frameSize=100 \
    --conf spark.default.parallelism=30 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --class com.yixia.hudong.spark.job.SqlJob \
    --executor-memory ${EXECUTOR_MEM}G \
    --num-executors ${EXECUTOR_NUM} \
    --executor-cores ${EXECUTOR_CPU} \
    --jars $LIBS \
    $LIB_DIR/audit-job-1.0-SNAPSHOT.jar $@