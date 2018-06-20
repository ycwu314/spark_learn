package com.example.secondarysort;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ycwu on 2018/6/20.
 */
public class SecondarySortExample {

    public static void main(String[] args) {
        //2015-03-30#6:55 AM#51#68#1#9506.21
        StructType transactionSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("time", DataTypes.StringType, false),
                DataTypes.createStructField("customerId", DataTypes.IntegerType, false),
                DataTypes.createStructField("productId", DataTypes.IntegerType, false),
                DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
                DataTypes.createStructField("total", DataTypes.DoubleType, false)
        ));
        String transactionPath = "src/main/resources/ch04/transactions.txt";

        try (SparkSession spark = SparkSession.builder().master("local").appName("SecondarySortExample").getOrCreate()) {
            JavaRDD<Row> transRDD = spark.read()
                    .option("delimiter", "#")
                    .schema(transactionSchema)
                    .csv(transactionPath)
                    .toJavaRDD();

            transRDD.mapToPair(row -> new Tuple2<>(new CustomerPriceKey(row.getInt(2), row.getDouble(5)), null))
                    .repartitionAndSortWithinPartitions(new CustomerPartitioner(10))
                    .collect()
                    .forEach(i -> System.out.println(i._1()) );
        }
    }
}