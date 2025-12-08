
1. How to create external Hive table without location?

**问题描述**

```
CREATE EXTERNAL TABLE IF NOT EXISTS new_table (id BIGINT, StartTime TIMESTAMP, EndTime TIMESTAMP) PARTITIONED BY (year INT, month INT, day INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
```
使用spark-sql创建外部表，不指定location时，会报如下错误:

```
Operation not allowed: CREATE EXTERNAL TABLE must be accompanied by LOCATION(line 1, pos 0)
```

spark版本2.2

**分析与答案**

The default SparkSqlParser (with astBuilder as SparkSqlAstBuilder) has the following assertion that leads to the exception:
```
if (external && location.isEmpty) {
  operationNotAllowed("CREATE EXTERNAL TABLE must be accompanied by LOCATION", ctx)
```

参考:  
https://stackoverflow.com/questions/44279412/how-to-create-external-hive-table-without-location