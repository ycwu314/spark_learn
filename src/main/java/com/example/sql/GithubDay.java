package com.example.sql;

import org.apache.parquet.Files;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by Administrator on 2018/6/18.
 */
public class GithubDay {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * broadcast + udf
     */
    @Test
    public void test() {
        try (SparkSession spark = SparkSession.builder().appName("GithubDay").master("local").getOrCreate();
             JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());) {
            Dataset<Row> githubDf = spark.read().json("src/main/resources/githubday/*.json.gz");

            List<String> employees = Files.readAllLines(new File("src/main/resources/ch03/ghEmployees.txt"), StandardCharsets.UTF_8);
            // 广播value
            Broadcast<List<String>> bcEmployees = javaSparkContext.broadcast(employees);

            // udf
            spark.sqlContext().udf().register("isEmployee", (String username) -> bcEmployees.value().contains(username), DataTypes.BooleanType);

            Dataset<Row> filteredByEmployee = githubDf.filter("type='PushEvent'")
                    .groupBy("actor.login")
                    .count()
                    .filter("isEmployee(login)")
                    .orderBy(new Column("count").desc());

            filteredByEmployee.show();

        } catch (IOException e) {
            logger.error("error: ", e);
        }
    }
}
