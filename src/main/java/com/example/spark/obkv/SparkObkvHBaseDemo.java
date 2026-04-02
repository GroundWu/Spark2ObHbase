package com.example.spark.obkv;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.spark.HBaseContext;

/**
 * Spark 读写 OBKV-HBase 示例。
 *
 * 运行前请确保：
 * 1. 已在 OceanBase 中创建表（见 README 建表 SQL）
 * 2. 在 src/main/resources/hbase-site.xml 中配置 OBKV 连接，或在本类中改用 ObkvHBaseConfig.forDirect/forCloud
 */
public class SparkObkvHBaseDemo {

    private static final String TABLE_NAME = "spark_demo";
    private static final String CF1 = "cf1";
    private static final String CF2 = "cf2";

    public static void main(String[] args) throws Exception {
        // 使用 classpath 下 hbase-site.xml，或改为 ObkvHBaseConfig.forDirect(...) / forCloud(...)
        Configuration hbaseConf = ObkvHBaseConfig.create();

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkObkvHBaseDemo")
                .setMaster("spark://6.12.233.103:7077");
        sparkConf.set("spark.sql.defaultUrlStreamHandlerFactory.enabled", "false");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        System.out.println("===== 1. RDD 写入 OBKV-HBase 再读取 =====");
        rddWriteAndRead(jsc, hbaseConf);

        // Spark SQL 方式需要 hbase-spark 兼容 Scala 2.12 的版本（官方 1.0.0 基于 Scala 2.11），暂跳过
        // System.out.println("===== 2. Spark SQL 读取 HBase =====");
        // sparkSqlRead(jsc, sparkConf, hbaseConf);

        jsc.stop();
    }

    /**
     * 使用 TableOutputFormat 写入、TableInputFormat 读取。
     */
    static void rddWriteAndRead(JavaSparkContext jsc, Configuration hbaseConf) throws Exception {
        String row1 = "row1", row2 = "row2", row3 = "row3";
        String family1 = CF1;
        String john_name = "John", john_age = "28", john_gender = "M";
        String lily_name = "Lily", lily_age = "25", lily_gender = "F";
        String walter_name = "Walter", walter_age = "35", walter_gender = "M";

        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        hbaseConf.set("mapreduce.job.output.key.class", ImmutableBytesWritable.class.getName());
        hbaseConf.set("mapreduce.job.output.value.class", Put.class.getName());
        hbaseConf.set("mapreduce.outputformat.class", TableOutputFormat.class.getName());

        JavaRDD<Tuple2<byte[], Tuple3<String, String, String>>> data = jsc.parallelize(Arrays.asList(
                new Tuple2<>(Bytes.toBytes(row1), new Tuple3<>(family1, "name", john_name)),
                new Tuple2<>(Bytes.toBytes(row1), new Tuple3<>(family1, "age", john_age)),
                new Tuple2<>(Bytes.toBytes(row1), new Tuple3<>(family1, "gender", john_gender)),
                new Tuple2<>(Bytes.toBytes(row2), new Tuple3<>(family1, "name", lily_name)),
                new Tuple2<>(Bytes.toBytes(row2), new Tuple3<>(family1, "age", lily_age)),
                new Tuple2<>(Bytes.toBytes(row2), new Tuple3<>(family1, "gender", lily_gender)),
                new Tuple2<>(Bytes.toBytes(row3), new Tuple3<>(family1, "name", walter_name)),
                new Tuple2<>(Bytes.toBytes(row3), new Tuple3<>(family1, "age", walter_age)),
                new Tuple2<>(Bytes.toBytes(row3), new Tuple3<>(family1, "gender", walter_gender))
        ));

        JavaPairRDD<ImmutableBytesWritable, Put> writeRDD = data.mapToPair(tuple -> {
            byte[] rowKey = tuple._1;
            String family = tuple._2._1;
            String qualifier = tuple._2._2;
            String value = tuple._2._3;
            Put put = new Put(rowKey);
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            return new Tuple2<>(new ImmutableBytesWritable(rowKey), put);
        });

        writeRDD.saveAsNewAPIHadoopDataset(hbaseConf);
        System.out.println("RDD 写入完成: " + TABLE_NAME);

        // 读取
        hbaseConf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(CF1));
        scan.addFamily(Bytes.toBytes(CF2));
        scan.setCaching(100);
        String scanStr = org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString(scan);
        hbaseConf.set(TableInputFormat.SCAN, scanStr);

        JavaPairRDD<ImmutableBytesWritable, Result> readRDD = jsc.newAPIHadoopRDD(
                hbaseConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        readRDD.flatMapToPair(tuple -> {
            ImmutableBytesWritable key = tuple._1;
            Result result = tuple._2;
            String rowKey = Bytes.toString(key.get());
            List<Tuple2<String, String>> res = new ArrayList<>();
            for (Cell c : result.rawCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(c));
                String column = Bytes.toString(CellUtil.cloneQualifier(c));
                res.add(new Tuple2<>(rowKey, String.format("K: %s, Q: %s, V: %s", rowKey, column, value)));
            }
            return res.iterator();
        }).collect().forEach(pair -> System.out.println(pair._2));
    }

    /**
     * 使用 Spark SQL 读取 HBase。OBKV-HBase 暂不支持 SparkSQLPushDownFilter，需关闭列下推。
     */
    static void sparkSqlRead(JavaSparkContext jsc, SparkConf sparkConf, Configuration hbaseConf) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLHBaseExample")
                .config(sparkConf)
                .getOrCreate();

        String tmpHdfsConfigPath = "/tmp/hbase-config-spark-demo";
        HBaseContext hbaseContext = new HBaseContext(jsc.sc(), hbaseConf, tmpHdfsConfigPath);

        String columnMapping = "id STRING :key, "
                + "name STRING cf1:name, "
                + "gender STRING cf1:gender, "
                + "age STRING cf1:age";

        Dataset<Row> df = spark.read()
                .format("org.apache.hadoop.hbase.spark")
                .option("hbase.columns.mapping", columnMapping)
                .option("hbase.spark.pushdown.columnfilter", false)
                .option("hbase.table", TABLE_NAME)
                .load();

        df.printSchema();
        df.show();

        df.createOrReplaceTempView(TABLE_NAME);
        Dataset<Row> result = spark.sql("SELECT * FROM " + TABLE_NAME + " WHERE age > 26");
        result.show();

        spark.stop();
    }

    /** Scala Tuple3 的 Java 简易替代 */
    static class Tuple3<T1, T2, T3> implements java.io.Serializable {
        final T1 _1;
        final T2 _2;
        final T3 _3;

        Tuple3(T1 _1, T2 _2, T3 _3) {
            this._1 = _1;
            this._2 = _2;
            this._3 = _3;
        }
    }
}
