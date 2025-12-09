
# Spark SQL支持Hive
1. 编译支持  
    -Phvie 和 -Phive-thriftserver构建一个包含Hive的新组件，Hive的新组件必须奋发到所有的Worker节点上。因为Worker节点需要访问Hive的serialization和deserialization库(SerDes),以便访问存储在Hive中的数据，所以包含Hive集合的jar必须拷贝到所有的worker节点。  
2. 运行支持Hive要点  
 (1) 配置hive-site.xml  
     将Hive配置信息hive-site.xml添加到$SPARK_HOME/conf  
 (2) 当运行一个Yarn集群时,datanucleus jars和hive-site.xml必须在Driver和全部的Executor启动.  
 (3) 可通过spark-submit命令通过--jars参数和--file参数加载.
 (4) 如无hive-site.xml配置文件, 仍可以创建一个HiveContext, 并会在当前目录下自动创建metastore_db和warehourse.(如在IDE下执行,则在当前工程根目录下)  

## Spark2整合Hive集群的方式有3种

	1.  传统方式是直接使用Hive集群的hive-site.xml原生配置,例如配置hive.metastore.warehouse.dir（在spark2.0+的版本，使用spark.sql.warehouse.dir），连接metadb所需要的驱动类，账户密码等，放置到$SPARK_HOME/conf下
	2.  创建hive-site.xml，通过配置目标hive集群的hive.metastore.uris，来与现有hive集群集成
	3. 不存在hive集群，也可使用hiveContext，会在当前目录下创建metastore_db与warehouse目录，可通过配置hive.metastore.warehouse.dir（在spark2.0+的版本，使用spark.sql.warehouse.dir）指定warehouse目录


# 运行

# 与Hive兼容性
<<Spark核心技术与高级应用>> P90 
补充Hive版本依赖关系列表  

# 支持Hive的特性
