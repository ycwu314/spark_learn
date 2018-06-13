package com.example.sql;

import com.model.ch04.Product;
import com.model.ch04.Transaction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

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


}
