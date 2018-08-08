package org.rohit.learn.spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by rramwal on 08/08/18.
 */
public class WordCountOptimized {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Word Count")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile("/Users/rramwal/gitdev/WordCountSpark/src/main/resources/wordCount.txt");
        JavaPairRDD<String, Integer> wordcounter = inputFile.flatMap(z -> Arrays.asList(z.replaceAll("[^a-zA-Z\\s]s?(\\s)?", "$1").split(" "))).mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> x + y);
        wordcounter.repartition(1).sortByKey().saveAsTextFile("/Users/rramwal/gitdev/WordCountSpark/src/main/resources/wordcountoptimized");

    }
}

