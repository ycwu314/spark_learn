package com.example.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Administrator on 2018/6/9.
 */
public class WordCount {

    public static void main(String[] args) {
        String path = "src/main/resources/LICENSE";
        try (SparkSession spark = SparkSession.builder().appName("WordCount").master("local").getOrCreate()) {
            JavaRDD<String> data = spark.read().textFile(path).toJavaRDD();
            data.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                    .filter(StringUtils::isAlpha)
                    .map(String::toLowerCase)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b)
                    .collect()
                    .forEach(t -> System.out.println(t._1 + "," + t._2));
        }
    }
}
