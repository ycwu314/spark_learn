package com.example.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2018/6/9.
 */
public class Java8WordCount {

    public static void main(String[] args) {
        List<String> input = Arrays.asList("Apache Spark is a fast and general-purpose cluster computing system. ",
                "It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. ",
                "It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, ",
                "MLlib for machine learning, GraphX for graph processing, and Spark Streaming.");

        m1(input);
        System.out.println(">>>>>>>>");

        m2(input);
    }

    private static void m1(List<String> input) {
        input.stream()
                .flatMap(line -> Arrays.stream(line.split("\\W+")))
                .filter(StringUtils::isAlpha)
                .map(word -> new Entry(word.toLowerCase(), 1))
                .collect(Collectors.groupingBy(Entry::getWord, Collectors.counting()))
                .forEach((k, v) -> System.out.println(k + "," + v));
    }

    private static void m2(List<String> input) {
        input.stream()
                .flatMap(line -> Arrays.stream(line.split("\\W+")))
                .filter(StringUtils::isAlpha)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .forEach((k, v) -> System.out.println(k + "," + v));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Entry {
        private String word;
        private int count;
    }
}
