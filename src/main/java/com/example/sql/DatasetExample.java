package com.example.sql;

import com.model.PV;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Administrator on 2018/6/23.
 */
public class DatasetExample {

    /**
     * create new column using existing column
     */
    @Test
    public void testWithColumn() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("DatasetExample").getOrCreate();) {
            Dataset<Integer> data = spark.createDataset(IntStream.range(1, 20).boxed().collect(Collectors.toList()), Encoders.INT());
            data.printSchema();
            // param: new column name; new column def
            data.withColumn("x10", data.col("value").multiply(10)).show();
        }
    }

    @Test
    public void testWindow() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("DatasetExample").getOrCreate();) {
            Dataset<PV> data = spark.createDataset(Arrays.asList(
                    new PV("站点2", new DateTime("2017-01-04").toDate(), 50),
                    new PV("站点1", new DateTime("2017-01-01").toDate(), 50),
                    new PV("站点1", new DateTime("2017-01-02").toDate(), 45),
                    new PV("站点1", new DateTime("2017-01-03").toDate(), 55),
                    new PV("站点2", new DateTime("2017-01-01").toDate(), 25),
                    new PV("站点2", new DateTime("2017-01-02").toDate(), 29),
                    new PV("站点2", new DateTime("2017-01-03").toDate(), 27)
            ), Encoders.bean(PV.class));

            // 指定数据分组和排序
            WindowSpec spec = Window.partitionBy("site").orderBy("date");


            data.withColumn("prev", lag(data.col("user_count"), 1).over(spec))
                    .withColumn("next", lead(data.col("user_count"), 1).over(spec))
                    // moving avg
                    .withColumn("moving_avg", avg(data.col("user_count")).over(spec.rowsBetween(-1, 1)))
                    // 分组排名
                    .withColumn("rank", rank().over(spec))
                    .show();

        }
    }
}
