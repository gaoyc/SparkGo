# DataFrame总结

## DataFrame操作
1. DataFrameAction操作  
collect, count, first, head, show, take(n)
2. 基础DataFrame函数
- cache, columns, explain, isLocal, persist, printSchema, registerTempTable, schema, toDF, unpersist
- dtypes: 以Array形式返回全部的列名和数据类型

3. 集成语言查询
   参考SQL语句, 如distinct, filter, groupBy, select, sort, where等
   **集成语言基础函数**
   - agg
   - select
   - where
   - distinct
   - cube
   - col
   - as
   - apply
   - drop
   - except
   - filter
   - groupBy
   - intersect
   - sort

4. 输出操作和RDD操作
   - 输出: 采用write保存DataFrame到外部存储
   - 支持RDD操作: coalesce, flatMap, foreach 等常见RDD操作

## RDD转化为DataFrame
1. 以反射机制推断RDD模式
    - 必须创建case类，只有case类才能隐式转换为DataFrame
    - 必须生成DataFrame, 进行注册临时表操作
    - 必须在内存中register成临时表，才能提供查询使用
2. 以编程方式定义RDD模式
    >当匹配模式(java的JavaBean， Python的kwargs字典)不能提前被定n用户分别设计属性义的场景.例如将不同用户设计不同属性
    step1. 从原始RDD创建一个RowS的RDD  
    step2. 创建一个StructType类型的Schema， 匹配第一步创建的RDD的RowS的结构  
    step3. 必须在内存中register成临时表，才能提供查询使用  
    
  *egg1: 以反射机制推断RDD模式*
```scala
    val conf = new SparkConf()
    val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //使用Case定义schema，属性不能超过22个
    case class Person(name: String, age: Int)

    // 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."
    val people = sc.textFile("file:///home/kigo/software/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")
      .map(_.split(",")).map(p=>Person(p(0), p(1).trim.toInt)).toDF //将数据写入Person模式类，隐式转换为DataFrame

    //DataFrame注册成为临时表
    people.registerTempTable("peopleTable")

    //使用Sql进行SQL表达式
    val result = sqlContext.sql("SELECT name, age FROM peopleTable WHERE age >= 13")

    result.map(row => "Name[get by index]="+row(0)).collect().foreach(println) //按字段索引获取
    result.map(row => "Name[get by fieldName]="+row.getAs[String]("name")).collect().foreach(println)  //按字段名称获取结果

```
  *egg2: 以编程方式定义RDD模式*
  
```scala
    val conf = new SparkConf()
    val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)

    // 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."
    val people = sc.textFile("file:///home/kigo/software/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")

    val schemaStr = "name age"
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType, StructField, StringType}
    //生成一个基于schemaStr结构的schema
    val schema = StructType(schemaStr.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRdd = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDF = sqlContext.createDataFrame(rowRdd, schema)

    //DataFrame注册成为临时表
    peopleDF.registerTempTable("peopleTable")

    //使用Sql进行SQL表达式
    val result = sqlContext.sql("SELECT name, age FROM peopleTable WHERE age >= 13")

    result.map(row => "Name[get by index]="+row(0)).collect().foreach(println) //按字段索引获取
    result.map(row => "Name[get by fieldName]="+row.getAs[String]("name")).collect().foreach(println)  //按字段名称获取结果

```

## 数据源
### 1. 加载与保存操作  
  - 加载: read
    `val df = sqlContext.read.load("file:///$SPARK_HOME/examples/src/main/resources/people.parquet")`  
    *备注*  
    　1) 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."  
    
  - 保存: read
    `df.write.save("saveUser.parquet")`
  - 要点
    - 指定选项
      >内置数据源可以使用短名称(json, parquet, jdbc), SparkSql支持通过format将任何类型的DataFrames转换成其他类型
      ```
      val df = sqlContext.read.format("json").load("somefile.json")
      val df.select("name", "age").write.format("parquet").save(otheFormat.parquet)
      ```
    - 保存模式
      - SaveMode.ErrorIfExists(default) 如果数据已存在，抛出异常
      - SaveMode.Append  如果数据存在，追加DataFrame数据
      - SaveMode.Overwrite 如果数据存在，重写
      - SaveMode.Ignore 如果数据存在，忽略
    
    - 保存为持久化表
      >当使用HiveContext时，DataFrames通过saveAsTable命令保存为持久化表使用.
      
### 2. Parquet文件
    *文件格式特点*
      - 支持多种数据处理系统的存储格式
      - 高效. Parquet采取列式存储，避免读入不需要的数据，具有极好性能及GC
      - 方便压缩和解压
      - 可以直接固化为Parquet文件，或者直接读取Parquet文件，具有比磁盘更好的缓存
  
  1). 加载数据编程
  ```scala
      val conf = new SparkConf()
      val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
      val sqlContext=new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
  
      //使用Case定义schema，属性不能超过22个
      case class Person(name: String, age: Int)
  
      // 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."
      val people = sc.textFile("file:///home/kigo/software/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")
        .map(_.split(",")).map(p=>Person(p(0), p(1).trim.toInt)).toDF //将数据写入Person模式类，隐式转换为DataFrame
  
      //将DataFrame保存为 parquet格式
      people.write.parquet("people.parquet")
      
      //加载parquet格式文件为DataFrame
      val parquetFile = sqlContext.read.parquet("people.parquet")
  
      //DataFrame注册成为临时表
      parquetFile.registerTempTable("penopleTable")
  
      //使用Sql进行SQL表达式
      val result = sqlContext.sql("SELECT name, age FROM peopleTable WHERE age >= 13")
  
      result.map(row => "Name[get by index]="+row(0)).collect().foreach(println) //按字段索引获取
      result.map(row => "Name[get by fieldName]="+row.getAs[String]("name")).collect().foreach(println)  //按字段名称获取结果
  
  ```
  
 2). 分区发现(artition discovery)
   >对于分区表，数据通常储存在不同的目录.Parquet数据源能够自动发现和推断分区信息。使用SQLContext.read的parquet或load名称，SparkSql自动提取分区信息。
   >分区列的数据类型是自动映射的，支持numeric数据类型和string类型的自动推断
 
 3). 模式合并(schema merging)
   >用户可以从简单的模式，根据需要逐渐添加更多的列。通过这种方式，用户最终得到多个不同但是能相互兼容模式的parquet额外年间，Parquet数据源能够自动检测这种情况，进而合并这些文件.
   >模式合并是相对昂贵的操作，1.5.0版本默认关闭
   ```scala
       val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
       val sqlContext=new org.apache.spark.sql.SQLContext(sc)
       //隐式转换一个RDD为dataFrame
       import sqlContext.implicits._
       //创建一个DataFrame，储存数据到一个分区目录
       val df1 = sc.makeRDD(1 to 5).map(i => (i, i*2)).toDF("single", "double")
       df1.write.parquet("data/test_table/key=1")
   
       //创建一个新DataFrame，储存数据到一个新分区目录
       val df2 = sc.makeRDD(6 to 10).map(i => (i, i*3)).toDF("single", "triple")
       df2.write.parquet("data/test_table/key=2")
   
       //读取分区表
       val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
       df3.printSchema()
   
       // The final schema consists of all 3 columns in the Parquet files together
       // with the partitioning column appeared in the partition directory paths.
       // root
       // |-- single: int (nullable = true)
       // |-- double: int (nullable = true)
       // |-- triple: int (nullable = true)
       // |-- key : int (nullable = true)
    ```
 
 4). 配置 
   - spark.sql.parquet.binaryAsString
     >false
   - spark.sql.parquet.int96AsTimestamp
     >true
   - spark.sql.parquet.cacheMetadata
     >true
   - spark.sql.parquet.compression.codec
     >gzip     
   - spark.sql.parquet.filterPushdown
     >true
   - spark.sql.hive.convertMetastoreParquet
     >true
   - spark.sql.parquet.output.committer.class
     >org.apache.parquet.hadoop.ParquetOutputCommitter
   - spark.sql.parquet.mergeSchema
     >false
     
### 3. JSON数据集
  - 通过SQLContext.read.json()方法使用json文件创建DataFrame
  - 通过转换个json对象的RDD\[String\]创建DataFrame
  **Egg**
  ```scala
  // sc is an existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  // A JSON dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val path = "examples/src/main/resources/people.json"
  val people = sqlContext.read.json(path)
  
  // The inferred schema can be visualized using the printSchema() method.
  people.printSchema()
  // root
  //  |-- age: integer (nullable = true)
  //  |-- name: string (nullable = true)
  
  // Register this DataFrame as a table.
  people.registerTempTable("people")
  
  // SQL statements can be run by using the sql methods provided by sqlContext.
  val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
  
  // 通过转换Jsonduix的RDD[String]创建DataFrame
  // Alternatively, a DataFrame can be created for a JSON dataset represented by
  // an RDD[String] storing one JSON object per string.
  val anotherPeopleRDD = sc.parallelize(
    """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
  val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
  ```
  
### 4. Hive表 
 **Egg**
 ```scala
 // sc is an existing SparkContext.
 val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
 
 sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
 sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
 
 // Queries are expressed in HiveQL
 sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
 ```
### 5. JDBC连接数据库
 - 指定驱动程序:
 SPARK_CLASSPATH=postgresql-9.3-1102-jdbc41.jar bin/spark-shell
 - JDBC支持的参数
   - url 	The JDBC URL to connect to. 
   - dbtable 	The JDBC table that should be read. Note that anything that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could also use a subquery in parentheses. 
   - driver 	The class name of the JDBC driver needed to connect to this URL. This class will be loaded on the master and workers before running an JDBC commands to allow the driver to register itself with the JDBC subsystem. 
   - partitionColumn, lowerBound, upperBound, numPartitions
     >These options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned. 
 
 **Egg**
 ```
 val jdbcDF = sqlContext.load("jdbc", Map(
   "url" -> "jdbc:postgresql:dbserver"
   "dbtable" -> "schema.tableName"
 ))
 ```
 
### 6. 多数据源整合
 >对于来自不同数据源，具有相同schema的DataFrame进行汇总操作. unionAll
 egg: `df1.unionAll(df2)`
 
# 分布式SQL Engine
  >SparkSQL作为一个分布式查询引擎，支持JDBC/ODBC，或者命令行接口，最终用户或者用户程序直接连接Spark Sql运行Sql查询
1. 运行Thrift JDBC/ODBC服务
  - 在Spark目录下执行如下命令, 启动JDBC/ODBC server
  `./sbin/start-thriftserver.sh`
  
  >通过环境变量修改HiveServer2端口(默认10000)
  ```
  export HIVE_SERVER2_THRIFT_PORT=<listening-port>
  export HIVE_SERVER2_THRIFT_BIND_HOST=<listening-host>
  ./sbin/start-thriftserver.sh \
    --master <master-uri> \
    ...
  ```
  
  >也可以通过-hiveconf指定Hive属性
  ```
  ./sbin/start-thriftserver.sh \
    --hiveconf hive.server2.thrift.port=<listening-port> \
    --hiveconf hive.server2.thrift.bind.host=<listening-host> \
    --master <master-uri>
    ...
  ```
  *备注：* 可通过下面help命令查看所有可用选项
  `./sbin/start-thriftserver.sh --help `
  
  - 使用beeline测试Thrift JDBC/ODBC服务
  `./bin/beeline`
  
  >Connect to the JDBC/ODBC server in beeline with:
  `beeline> !connect jdbc:hive2://localhost:10000`
  即可以在Hvie的beelin中直接使用SparkSQL
  
  将hive-site.xml放置到spark的conf/目录下，就完成了hive的配置

  *通过HTTP发送thrift RPC 消息*
  参考官方文档《Distributed SQL Engine》一节
  
2. 运行Spark SQL CLI
  The Spark SQL CLI is a convenient tool to run the Hive metastore service in local mode and execute queries input from the command line. Note that the Spark SQL CLI cannot talk to the Thrift JDBC server.
  To start the Spark SQL CLI, run the following in the Spark directory:
  `./bin/spark-sql`
