package com.example.sql;

import com.model.ch04.Product;
import com.model.ch04.Transaction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ycwu on 2018/6/13.
 */
public class JoinExample {

    // 1#ROBITUSSIN PEAK COLD NIGHTTIME COLD PLUS FLU#9721.89#10
    private StructType productSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("total", DataTypes.DoubleType, false),
            DataTypes.createStructField("amount", DataTypes.IntegerType, false)
    ));

    //2015-03-30#6:55 AM#51#68#1#9506.21
    private StructType transactionSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("date", DataTypes.DateType, false),
            DataTypes.createStructField("time", DataTypes.StringType, false),
            DataTypes.createStructField("customerId", DataTypes.IntegerType, false),
            DataTypes.createStructField("productId", DataTypes.IntegerType, false),
            DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
            DataTypes.createStructField("total", DataTypes.DoubleType, false)
    ));

    private String productPath = "src/main/resources/ch04/products.txt";
    private String transactionPath = "src/main/resources/ch04/transactions.txt";

    @Test
    public void testSqlJoin() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("JoinExample").getOrCreate()) {
            Dataset<Row> productDF = spark.read().option("delimiter", "#").schema(productSchema).csv(productPath).toDF();
            Dataset<Row> transactionDF = spark.read().option("delimiter", "#").schema(transactionSchema).csv(transactionPath).toDF();

            productDF.createOrReplaceTempView("product");
            transactionDF.createOrReplaceTempView("transaction");

            spark.sql("SELECT * FROM transaction t, product p WHERE p.id = t.productId").show();
        }
    }

    @Test
    public void testRDDJoin() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("JoinExample").getOrCreate()) {
            Dataset<Row> productDF = spark.read().option("delimiter", "#").schema(productSchema).csv(productPath);
            Dataset<Row> transactionDF = spark.read().option("delimiter", "#").schema(transactionSchema).csv(transactionPath).toDF();

            // convert to pair rdd, so that can join
            JavaPairRDD<Integer, Product> productPairRDD = productDF.toJavaRDD().mapToPair(row ->
                    new Tuple2<>(row.getInt(0), new Product(row.getInt(0), row.getString(1), row.getDouble(2), row.getInt(3)))
            );

            JavaPairRDD<Integer, Transaction> transactionPairRDD = transactionDF.toJavaRDD().mapToPair(row ->
                    new Tuple2<>(row.getInt(3), new Transaction(row.getDate(0), row.getString(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getDouble(5)))
            );

            transactionPairRDD.join(productPairRDD).collect().forEach(i -> System.out.println(i));

        }
    }

    /**
     * cogroup vs join。cogroup类似full outer join，但是返回结果为2个Iterable。
     */
    @Test
    public void testCogroup() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("JoinExample").getOrCreate();
             JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            // 作用于pair RDD
            JavaPairRDD<String, Integer> a1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<>("A", 1), new Tuple2<>("B", 2), new Tuple2<>("C", 3)));
            JavaPairRDD<String, Integer> a2 = jsc.parallelizePairs(Arrays.asList(new Tuple2<>("A", 10), new Tuple2<>("D", 40), new Tuple2<>("E", 5)));

            a1.cogroup(a2).collect().forEach(i -> System.out.println(i._1 + "," + i._2));
        }
    }

    /**
     * zip要求两个分区的paritition数量和元素数量相同
     */
    @Test
    public void testZip() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("JoinExample").getOrCreate();
             JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<Integer> a1 = jsc.parallelize(IntStream.range(0, 20).boxed().collect(Collectors.toList()), 3);
            JavaRDD<Integer> a2 = jsc.parallelize(IntStream.range(30, 50).boxed().collect(Collectors.toList()), 3);
            JavaRDD<Integer> a3 = jsc.parallelize(IntStream.range(30, 100).boxed().collect(Collectors.toList()), 3);

            JavaRDD<Integer> a4 = jsc.parallelize(IntStream.range(30, 50).boxed().collect(Collectors.toList()), 4);

            a1.zip(a2).collect().forEach(i -> System.out.println(i._1() + ", " + i._2()));

            try {
                a1.zip(a3).collect();
            } catch (Exception e) {
                System.err.println("a1 zip a3 has error");
                e.printStackTrace();
            }

            try {
                a1.zip(a4).collect();
            } catch (Exception e) {
                System.err.println("a1 zip a4 has error");
                e.printStackTrace();
            }
        }
    }

    /**
     * zipPartitions只要求分区数量一致，不要求元素个数相同
     */
    @Test
    public void testZipPartitions() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("JoinExample").getOrCreate();
             JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<Integer> a1 = jsc.parallelize(IntStream.range(0, 20).boxed().collect(Collectors.toList()), 3);
            JavaRDD<Integer> a4 = jsc.parallelize(IntStream.range(30, 100).boxed().collect(Collectors.toList()), 3);

            a1.zipPartitions(a4, (FlatMapFunction2<Iterator<Integer>, Iterator<Integer>, Tuple2<Integer, Integer>>) (it, it2) -> {
                List<Tuple2<Integer, Integer>> result = new ArrayList<>();
                while (it.hasNext() && it2.hasNext()) {
                    result.add(new Tuple2<>(it.next(), it2.next()));
                }
                while (it.hasNext()) {
                    result.add(new Tuple2<>(it.next(), null));
                }
                while (it2.hasNext()) {
                    result.add(new Tuple2<>(null, it2.next()));
                }

                return result.iterator();
            }).collect().forEach(i -> System.out.println(i._1() + ", " + i._2()));
        }
    }
}
