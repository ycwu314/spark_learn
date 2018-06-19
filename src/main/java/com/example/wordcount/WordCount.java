package com.example.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2018/6/9.
 */
public class WordCount {

    String path = "src/main/resources/LICENSE";

    @Test
    public void testWordCountV1() {
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

    /**
     * 使用countByValue()
     */
    @Test
    public void testWordCountV2() {
        try (SparkSession spark = SparkSession.builder().appName("WordCount").master("local").getOrCreate()) {
            JavaRDD<String> data = spark.read().textFile(path).toJavaRDD();
            data.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                    .filter(StringUtils::isAlpha)
                    .map(String::toLowerCase)
                    .countByValue()
                    .forEach((k, v) -> System.out.println(k + "," + v));
        }
    }

    /**
     * 使用mapPartitions()。把每个分区的元素打包为Iterator
     */
    @Test
    public void testWordCountV3() {
        try (SparkSession spark = SparkSession.builder().appName("WordCount").master("local").getOrCreate()) {
            JavaRDD<String> data = spark.read().textFile(path).toJavaRDD();
            data.mapPartitions((FlatMapFunction<Iterator<String>, String>) stringIterator -> {
                List<String> result = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String line = stringIterator.next();
                    result.addAll(Arrays.asList(line.split("\\W+")));
                }
                return result.iterator();
            }).filter(StringUtils::isAlpha)
                    .mapPartitions((FlatMapFunction<Iterator<String>, String>) stringIterator -> {
                        List<String> result = new ArrayList<>();
                        while (stringIterator.hasNext()) {
                            String word = stringIterator.next();
                            result.add(word.toLowerCase());
                        }
                        return result.iterator();
                    }).countByValue()
                    .forEach((k, v) -> System.out.println(k + "," + v));
        }
    }
}
