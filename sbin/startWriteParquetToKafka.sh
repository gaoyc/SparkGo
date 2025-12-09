#!/bin/sh

appRoot="$(cd "`dirname "$0"`"/..; pwd)"
echo "app root dir="$appRoot

source $appRoot/sbin/common-env.sh

# 指定应用配置文件，需要classpath下

SPARK_HOME=/opt/spark-2.2.0-bin-hadoop2.7

# 独立部署的Spark版本需要配置HADOOP_CONF_DIR or YARN_CONF_DIR
export YARN_CONF_DIR=/etc/hadoop/conf
# 集成hadoop/yarn classpath，以支持yarn模式提交任务
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

# Parquet 文件路径
parquetPath="hdfs://host168:8020/tmp/fineweb-edu-dataset"
# Kafka 集群地址
kafkaBootstrapServers="host167:9092,host168:9092,host169:9092"
# Kafka Topic 名称
kafkaTopic="fineweb_edu_dataset.V001"

appArgs="${parquetPath} ${kafkaBootstrapServers} ${kafkaTopic}"

echo "==appArgs:$appArgs"

MainJar=`find $appRoot -name "sparkGo-*.jar`


function startTaskBySparkSubmit(){
mainClass=com.kigo.bigdata.spark.utils.WriteParquetToKafka
    ${SPARK_HOME}/bin/spark-submit \
    --class $mainClass \
    --master local[*] \
    --num-executors  3 \
    --executor-memory 16g \
    --driver-memory 16g \
    --executor-cores 3 \
    --total-executor-cores 30 \
    --driver-class-path $vClasspath \
    --queue default \
    --files ${vConfigs} \
    --jars $JARS \
    --driver-java-options ${driverJavaOptions} \
#    --principal ${vPrincipal} \
#    --keytab ${vKeytabPath} \
    --conf spark.core.connectionack.wait.timeout=7000 \
    --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
#    --conf spark.yarn.security.credentials.hbase.enabled=true \
    --conf spark.yarn.driver.memoryOverhead=32g \
    --conf spark.yarn.executor.memoryOverhead=32g \
    $MainJar ${appArgs} $@

# 注释配置
# --files ${appRoot}/conf/xxx.conf \

}


checkEnv
getJars
printArgs
#startByJava "$@"
#startTaskBySparkSubmit "$@" 2>&1 &
startTaskBySparkSubmit "$@"
pid=$!
echo $pid > ../pid-mergeoffline
echo "PID="$pid

