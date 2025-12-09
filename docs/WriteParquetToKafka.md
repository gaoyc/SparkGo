
# 总结

# env
部署路径：192.18.230.167:/opt/merge_track_standalone-v1.0

# spark 独立部署模式

## 集成支持hadoop

编辑bin/spark-env.sh，追加修改如下配置

```
export HADOOP_HOME=${HADOOP_HOME:-/usr/hdp/4.0.0.0-78/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/usr/hdp/4.0.0.0-78/hadoop/conf}

# 集成hadoop/yarn classpath，以支持yarn模式提交任务
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
```
## cmd-常用命令

```bash
[root@host167 spark-2.2.0-bin-hadoop2.7]# bin/spark-sql --master yarn --conf spark.executor.memory=16G --conf spark.driver.memory=16G --conf spark.executor.instances=9
[root@host167 spark-2.2.0-bin-hadoop2.7]# bin/spark-shell --master yarn --conf spark.executor.memory=16G --conf spark.driver.memory=16G --conf spark.executor.instances=9

spark-sql> val parquetPath="hdfs://host168:8020/tmp/fineweb-edu-dataset/00000.parquet"
spark-sql> val df = spark.read.parquet(parquetPath)
spark-sql> df.show(2)
spark-sql> df.count()
```


# 创建topic相关
```bash
# on host167
KAFKA_HOME=/usr/hdp/current/kafka-broker
ZOOKEEPER_LIST=host167:2181,host168:2181,host169:2181
bootstrap_server=host167:9092,host168:9092,host169:9092

BROKER_LIST=host167:9092,host168:9092,host169:9092

topic_name=fineweb_edu_dataset.V001

consumer_group=console_consumer_test

# 列出所有topic
## kafka v2
$KAFKA_HOME/bin/kafka-topics.sh  --list  --zookeeper $ZOOKEEPER_LIST
## kafka v3
$KAFKA_HOME/bin/kafka-topics.sh  --list  --bootstrap-server $bootstrap_server

# 创建topic
## kafka v2
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_LIST --create --replication-factor 2 --partitions 60  --topic $topic_name
## kafka v3
$KAFKA_HOME/bin/kafka-topics.sh  --create --replication-factor 2 --partitions 60  --topic $topic_name --bootstrap-server $bootstrap_server

# 删除topic
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_LIST --delete -topic $topic_name
## kafka v3
$KAFKA_HOME/bin/kafka-topics.sh --delete -topic $topic_name  --bootstrap-server $bootstrap_server

# 消费
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic $topic_name  --bootstrap-server $bootstrap_server --group $consumer_group --from-beginning  --max-messages 5

# 查看指定topic信息
$KAFKA_HOME/bin/kafka-topics.sh --describe  --topic $topic_name --bootstrap-server $bootstrap_server

# 查看指定消费积压-偏移量。查看topic有效偏移量, 消费者组要存在，可以先用console消费
## kafak v2
$KAFKA_HOME/bin/kafka-consumer-offset-checker.sh  --topic $topic_name --group group_name --bootstrap-server $bootstrap_server

$KAFKA_HOME/bin/kafka-consumer-offset-checker.sh  --topic $topic_name --group group_name --zookeeper $ZOOKEEPER_LIST

$KAFKA_HOME/bin/kafka-consumer-groups.sh   --bootstrap-server $bootstrap_server --topic $topic_name --group group_name
## kafka v3
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server $bootstrap_server -describe --group $consumer_group




# 查看 Topic 的最早偏移量（Log Start Offset）和最新偏移量（Log End Offset）
#time：-1表示获取最早偏移量；-2表示获取最晚偏移量；默认-1
# v2
$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKER_LIST --topic $topic_name --time time_para

# v3
## 默认获取最新
$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $bootstrap_server \
  --topic $topic_name
  
$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $bootstrap_server \
  --topic $topic_name --time -1  


```


