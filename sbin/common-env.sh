#!/bin/sh
##################################
appRoot="$(cd "`dirname "$0"`"/..; pwd)"
echo "app root dir="$appRoot
echo "user.keytab: $user.keytab userprincipal: $vPrincipal"
#############################################################
# =====请根据实际修改如下配置
# 华为客户端安装根目录，如存在，后自动加载客户端环境变量
huaweiFiClient=/opt/client
# 华为安全配置keytab文件。如非华为安全，可忽略
vKeytabPath=${appRoot}/conf/user.keytab
# 华为安全用户。如非华为安全，可忽略
#vPrincipal=Teligen_M
vPrincipal=ds_test

#导出hadoop native(snappy动态库)到环境变量
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/client/HBase/hbase/lib/native
# hadoopNative=$appRoot/lib/native
hadoopNative=${huaweiFiClient}/HBase/hbase/lib/native

#集群规模标识
cluster_scale_level=1

#根据集群规模设置程序所需资源
case $cluster_scale_level in
	1)
	  num_executors=30 #yarn-client模式使用
	  executor_cores=3
	  executor_memory=10g
	  driver_memory=8g
	;;
	2)
	  num_executors=60
	  executor_cores=3
	  executor_memory=10g
	  driver_memory=16g
	;;
	*)
	  num_executors=300
	  executor_cores=10
	  executor_memory=30g
	  driver_memory=32g
	;;
esac
#若调度模式为standalone该变量起作用
total_executor_cores=$((num_executors*executor_cores))

##################################



##-------------参数配置--------------##
#CLIENT_VM="-Xms8 -Xmx8G   -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+PrintGCDetails  -XX:+PrintGCTimeStamps  -XX:+PrintGCDateStamps  -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=10M  -Xloggc:$appRoot/logs/gc.log   -XX:MaxDirectMemorySize=4g  -XX:SurvivorRatio=8   -XX:NewRatio=2   -XX:MaxMetaspaceSize=4G" #华为客户端安装路径
MainJar=`find $appRoot -name "MainJarFlg*.jar"`
#MainJar=$appRoot/lib/StreamingLoader.jar
#vm启动参数
VmFlags=" -Dlog4j.configuration=file:${appRoot}/conf/log4j.properties "
VmFlags="$CLIENT_VM $VmFlags"

#driverJavaOptions=" -Xms8G -Xmx8G -XX:NewSize=2048m -XX:MaxNewSize=2048m "
driverJavaOptions="  "

#导入华为FI环境变量
if [ -f ${huaweiFiClient}/bigdata_env ]
then
  source ${huaweiFiClient}/bigdata_env
  kinit -kt $vKettabPath $vPrincipal
  #echo "===================disable source huawei bigdata_env for test for temp"
  echo "===================disable source huawei bigdata_env for test for temp"
else
  echo "there is no huawei fi client on the path: "${huaweiFiClient}/bigdata_env
fi
hbaseLib=${huaweiFiClient}/HBase/hbase/lib
#export SPARK_CLASSPATH=${hbaseLib}:${huaweiFiClient}/Spark/spark/lib/streamingClient:${SPARK_CLASSPATH}

#如以YARN运行spark，华为FI版本不需要YARN_CONF_DIR,但开源版本需要设置
#export YARN_CONF_DIR=${huaweiFiClient}/HBase/hbase/conf
#export YARN_CONF_DIR=/opt/client/HDFS/hadoop/etc/hadoop
#export YARN_CONF_DIR=$appRoot/conf/hbase
#export HADOOP_CONF_DIR=$appRoot/conf/hbase
echo "=====================YARN_CONF_DIR="$YARN_CONF_DIR
echo "=====================HADOOP_CONF_DIR="$HADOOP_CONF_DIR

#export SPARK_CONF_DIR=$appRoot/conf

##===============以下如参数无特殊情况可不需修改============
sparkPropertiesFile=conf/spark-defaults.conf
ulimit -c unlimited

JARS=$MainJar
vConfigs=""
vClasspath=$appRoot

##-------------END-通用参数配置--------------##
#############################################################

function checkEnv(){
    if [ -f $hadoopNative/libsnappy.so ]
    then
    	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$hadoopNative
    else
      echo "error!! the hadoop native path does not exist!! path=$hadoopNative"
    	# exit 1
    fi

    # Find the java binary
    if [ -n "${JAVA_HOME}" ]; then
      RUNNER="${JAVA_HOME}/bin/java"
    else
      if [ `command -v java` ]; then
        RUNNER="java"
      else
        echo "JAVA_HOME is not set" >&2
        exit 1
      fi
    fi

    #检查创建日志目录
    if [ ! -d ${appRoot}/logs ]
    then
    	mkdir -p ${appRoot}/logs
    	echo "success to create the logs dir: ${appRoot}/logs"
    fi
}

function getJars(){
  for jar in `find $appRoot/lib -maxdepth 3 -iname \*.jar |grep -v "\(bak\|backup\)"`
  do
    JARS=$JARS,$jar
  done
}

function getConfigs(){
  for configFile in `find $appRoot/conf -maxdepth 2 -type f |grep -v "\(bak\|backup\)" |grep -v "keytab"`  # 因spark-submit指定--keytab，则需要排除
  do
    vConfigs=$configFile,$vConfigs
  done
}

function getDriverClasspath(){
  for path in `find $appRoot/lib -maxdepth 2 -type d | grep -v "\(bak\|backup\)"`
  do
    vClasspath=$vClasspath:$path/*
  done

  for path in `find $appRoot/conf -maxdepth 2 -type d | grep -v "\(bak\|backup\)"`
  do
    vClasspath=$vClasspath:$path
  done

}


function printArgs(){
  echo "--jars="$JARS
  echo ""

  getConfigs
  echo "--files="$vConfigs
  echo ""

  getDriverClasspath
  echo "--driver-class-path="$vClasspath
  echo ""

  driverJavaOptions="${driverJavaOptions}  -Dlog4j.configuration=file:${appRoot}/conf/log4j.properties"
  echo "--driver-java-options=${driverJavaOptions}"
  echo ""
  echo "--spark-properties-file=${appRoot}/${sparkPropertiesFile}"
  echo ""
  echo "--driver-class-path="$vClasspath
  echo ""
}

#--properties-file ${appRoot}/${sparkPropertiesFile} \
#如table_d.conf设置了master为yarn
function startBySparkSubmit(){
#mainClass=org.teligen.store.util.dstream.KafkaStreamToFile
mainClass=org.teligen.store.util.etl.CarbondataExport
    /opt/client/Spark2x/spark/bin/spark-submit \
    --class $mainClass \
    --master yarn \
    --num-executors  10\
    --executor-memory 10g \
    --driver-memory 10g \
    --executor-cores 3 \
    --total-executor-cores 30 \
    --driver-class-path $vClasspath \
    --queue default \
    --files ${appRoot}/conf/export/exportHtable.conf \
    --jars $JARS \
    --driver-java-options ${driverJavaOptions} \
    --principal ds_test \
    --keytab ${appRoot}/conf/hbase/user.keytab \
    --conf spark.core.connectionack.wait.timeout=7000 \
    --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
    --conf spark.yarn.security.credentials.hbase.enabled=true \
    $MainJar $@ 
    
}
# >> ${appRoot}/logs/sysout.log  2>&1 &
# --properties-file ${appRoot}/${sparkPropertiesFile} \
# vClasspath=$MainJar:$appRoot/lib:$vClasspath

function startByJava(){
    #mainClass=org.teligen.store.test.LoaderFromFileTest
    #mainClass=org.teligen.store.StreamingLoader
    #mainClass=org.teligen.store.util.dstream.KafkaStreamToFile
    mainClass=org.teligen.store.util.etl.HbaseExport
    echo "--Main vm flags="$VmFlags
    #nohup java $VmFlags -cp $vClasspath org.apache.spark.deploy.SparkSubmit $vSparkArgs  $@ |tee ${appRoot}/logs/sysout.log  2>&1 &
    #nohup java $VmFlags -cp $vClasspath org.apache.spark.deploy.SparkSubmit $vSparkArgs  $@ > ${appRoot}/logs/sysout.log  2>&1 &
    java $VmFlags -cp $vClasspath org.apache.spark.deploy.SparkSubmit $vSparkArgs  $@ 
}



