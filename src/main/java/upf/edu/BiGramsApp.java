package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.io.FileWriter;
import java.io.IOException;

//spark-submit --class upf.edu.BiGramsApp --master local[*] target/lab2-4-1.0-SNAPSHOT.jar es ./dataset/out/ ./dataset/Eurovision9.json

public class BiGramsApp {
    public static void main(String[] args) throws IOException {
        List<String> argsList = Arrays.asList(args);
        String lang = argsList.get(0);
        String outputDir = argsList.get(1);
        String input = argsList.get(2);


        long start = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("Twitter Filter");
        conf.setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(input)
                .filter(sentence -> !sentence.isEmpty())
                .map(word -> ExtendedSimplifiedTweet.fromJson(word)) // Get Optional from tweet json
                .filter(s-> s.isPresent()) // Filter only present
                .map(s -> s.get().getText()) // Optional to SimplifiedTweet
                //.filter(s -> s.getLanguage().equals(lang)) // Filter language
                //.map(s -> s.toString()) // SimplifiedTweet to String
                .map(word -> normalise(word)); //normalizamos el texto


        JavaPairRDD<String, Integer> counts = sentences
                .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
                .map(word -> normalise(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        JavaPairRDD<Integer, String> swapped = counts
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap)
                .sortByKey(false);

        swapped.saveAsTextFile(outputDir);
    }

    private static String normalise(String word) {

        return word.trim().toLowerCase();

    }
}
