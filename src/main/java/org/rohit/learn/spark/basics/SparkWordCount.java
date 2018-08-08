/**
 * Created by rramwal on 05/08/18.
 */
package org.rohit.learn.spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;


public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Word Count")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sparkContext.textFile("/Users/rramwal/gitdev/WordCountSpark/src/main/resources/wordCount.txt");

        //convert to non empty lines
        JavaRDD<String> nonEmptyLines = input.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !(s == null || s.trim().length() < 1);
            }

        });

        ///flatten the words
        JavaRDD<String> flattenedWords = nonEmptyLines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.replaceAll("[^a-zA-Z\\s]s?(\\s)?", "$1").split(" "));
            }
        });


        ////convert words to tuple
        JavaPairRDD<String, Integer> wordToPair = flattenedWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s, 1);
            }
        });

        ////reduce tuple based on count of words
        JavaPairRDD<String, Integer> wordCount = wordToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordCount.repartition(1).sortByKey().saveAsTextFile("/Users/rramwal/gitdev/WordCountSpark/src/main/resources/wordCount_output");
        sparkContext.close();


    }

}
