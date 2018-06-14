package com.example.sql;

import com.model.AvgHolder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/**
 * Created by ycwu on 2018/6/14.
 */
public class ByKeyExample implements Serializable {

    private List<String> input = Arrays.asList("hello", "moto", "hello", "nokia");

    /**
     * for pair (K, V1), (k, V2) where V1 and V2 are of the same type, return (k, V3=f(V1,V2)) where V3 and V1, V2 are of the same type
     */
    @Test
    public void testReduceByKey() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("ReduceByKeyExample").getOrCreate()) {
            JavaRDD<String> data = spark.createDataset(input, Encoders.STRING()).toJavaRDD();
            data.mapToPair(row -> new Tuple2<>(row, 1))
                    .reduceByKey((a, b) -> a + b)
                    .collect()
                    .forEach(i -> System.out.println(i._1() + "," + i._2()));
        }
    }

    @Test
    public void testFoldByKey() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("FoldByKeyExample").getOrCreate()) {
            JavaRDD<String> data = spark.createDataset(input, Encoders.STRING()).toJavaRDD();
            data.mapToPair(row -> new Tuple2<>(row, 1))
                    .foldByKey(0, (a, b) -> a + b)
                    .collect()
                    .forEach(i -> System.out.println(i._1() + "," + i._2()));
        }
    }

    /**
     * https://github.com/databricks/learning-spark/blob/master/src/main/java/com/oreilly/learningsparkexamples/java/BasicAvg.java
     * <p>
     * 计算平均数
     */
    @Test
    public void testAggregate() {
        try (SparkSession spark = SparkSession.builder().master("local").appName("AggregateExample").getOrCreate();
             JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<Integer> data = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 3);
            // seqOp
            Function2<AvgHolder, Integer, AvgHolder> addAndCount = (Function2<AvgHolder, Integer, AvgHolder>) (v1, v2) -> {
                v1.setNum(v1.getNum() + 1);
                v1.setTotal(v1.getTotal() + v2);
                return v1;
            };
            // combineOp
            Function2<AvgHolder, AvgHolder, AvgHolder> combine = (Function2<AvgHolder, AvgHolder, AvgHolder>) (v1, v2) -> {
                v1.setTotal(v1.getTotal() + v2.getTotal());
                v1.setNum(v1.getNum() + v2.getNum());
                return v1;
            };

            AvgHolder avgHolder = data.aggregate(new AvgHolder(0, 0), addAndCount, combine);
            System.out.println("total=" + avgHolder.getTotal() + ", num=" + avgHolder.getNum() + ", avg=" + (avgHolder.getTotal() / avgHolder.getNum()));
        }
    }


}
