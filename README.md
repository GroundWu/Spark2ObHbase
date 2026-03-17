# Spark 读写 OBKV-HBase Demo

基于 [Spark 使用 OBKV-HBase 指导文档](https://yuque.antfin.com/ob/gtuwei/gy6qcznx653mqklu) 编写的示例工程，演示使用 Spark 通过 OBKV-HBase 访问 OceanBase 宽表（写后读、Spark SQL 读）。

## 前提条件

- **OBKV-HBase Java 客户端** 2.2.1+（本工程使用 2.2.1）
- **OceanBase 服务端** 4.3.5.3+
- 已在 OceanBase 中创建好表（见下方建表 SQL）

## 建表（OceanBase 中执行）

```sql
CREATE TABLEGROUP `spark_demo` SHARDING = 'ADAPTIVE';

CREATE TABLE `spark_demo$cf1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = `spark_demo` partition by key(`K`) partitions 17;

CREATE TABLE `spark_demo$cf2` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = `spark_demo` partition by key(`K`) partitions 17;
```

## 配置连接

在 **直连** 与 **云上** 二选一。

### 方式一：`hbase-site.xml`（推荐）

编辑 `src/main/resources/hbase-site.xml`：

- **直连模式**：填写 `hbase.oceanbase.paramURL`、`fullUserName`、`password`、`sysUserName`、`sysPassword`。  
  **注意**：`paramURL` 中的 `&` 必须写成 `&amp;`，否则解析失败。
- **云上模式**：填写 `fullUserName`、`password`、`odpAddr`、`odpPort`（一般 3307）、`database`，并启用 `odpMode=true`。

### 方式二：代码中设置

在 `SparkObkvHBaseDemo.java` 的 `main` 中，将：

```java
Configuration hbaseConf = ObkvHBaseConfig.create();
```

改为直连示例：

```java
Configuration hbaseConf = ObkvHBaseConfig.forDirect(
    "YOUR_PARAM_URL",           // 从 obconfig 获取 RS 列表的 URL
    "root@obkv#obkvcluster",    // userName@tenantName#clusterName
    "your_password",
    "sysroot",
    "sys_password"
);
```

或云上示例：

```java
Configuration hbaseConf = ObkvHBaseConfig.forCloud(
    "root",       // 用户名
    "password",
    "your-odp-host",
    3307,
    "test"        // database
);
```

## 编译与运行

### 方式一：直接运行（本地，无需 spark-submit）

配置好 OBKV 连接后，在项目根目录执行：

```bash
mvn compile exec:java -Dexec.mainClass="com.example.spark.obkv.SparkObkvHBaseDemo"
```

或在 IDE 中直接运行 `SparkObkvHBaseDemo` 的 main 方法。默认使用 `local[*]`，无需 Spark 集群。

### 方式二：打包后一条命令提交

```bash
mvn clean package -DskipTests
```

会得到两个 jar：普通 jar 和 **带依赖的 fat jar**（`target/spark-obkv-hbase-demo-1.0-SNAPSHOT-with-deps.jar`）。用 fat jar 可直接提交，无需 `--jars`：

```bash
# 本地
$SPARK_HOME/bin/spark-submit \
  --class com.example.spark.obkv.SparkObkvHBaseDemo \
  target/spark-obkv-hbase-demo-1.0-SNAPSHOT-with-deps.jar

# 提交到集群（指定 master）
$SPARK_HOME/bin/spark-submit \
  --class com.example.spark.obkv.SparkObkvHBaseDemo \
  --master spark://your-master:7077 \
  target/spark-obkv-hbase-demo-1.0-SNAPSHOT-with-deps.jar
```

**示例：提交到固定集群**（如 `spark://6.12.233.103:7077`）

```bash
$SPARK_HOME/bin/spark-submit \
  --class com.example.spark.obkv.SparkObkvHBaseDemo \
  --master spark://6.12.233.103:7077 \
  target/spark-obkv-hbase-demo-1.0-SNAPSHOT-with-deps.jar
```

通过系统属性指定 master（覆盖代码默认的 `local[*]`）：

```bash
mvn exec:java -Dexec.mainClass="com.example.spark.obkv.SparkObkvHBaseDemo" -Dspark.master=spark://host:7077
```

## 示例说明

1. **RDD 写 + 读**  
   使用 `TableOutputFormat` 写入三条 row（row1/row2/row3，列 cf1:name、cf1:age、cf1:gender），再用 `TableInputFormat` + `Scan` 读回并打印。

2. **Spark SQL 读**  
   使用 `hbase-spark` 将表映射为 DataFrame（`:key` → id，cf1 列 → name、gender、age），并执行 `WHERE age > 26`。  
   **注意**：OBKV-HBase 当前不支持 `SparkSQLPushDownFilter`，示例中已设置 `hbase.spark.pushdown.columnfilter=false`。

## 依赖说明

- **obkv-hbase-client**：排除 `jackson-databind`，避免与 Spark 自带版本冲突。
- **hbase-spark**：排除 `hbase-client` 和 `hadoop-common`，由 OBKV-HBase 客户端提供，避免版本冲突。

## 参考

- 语雀文档：[Spark 使用 OBKV-HBase 指导文档](https://yuque.antfin.com/ob/gtuwei/gy6qcznx653mqklu)
- [使用 OBKV-HBase 客户端连接集群](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000002022354)
- [OBKV-HBase 数据操作示例](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000002022353)
