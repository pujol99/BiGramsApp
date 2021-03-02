package upf.edu;

import com.amazonaws.jmespath.OpGreaterThan;
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


//spark-submit --class upf.edu.MostRetweetedApp --master local[*] target/lab2-4-1.0-SNAPSHOT.jar ./dataset/out/ ./dataset/Eurovision9.json

public class MostRetweetedApp {
    public static void main(String[] args) throws IOException {
        List<String> argsList = Arrays.asList(args);
        String outputDir = argsList.get(0);
        String input = argsList.get(1);


        long start = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("Twitter Filter");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(input)
                .filter(sentence -> !sentence.isEmpty())
                .map(word -> ExtendedSimplifiedTweet.fromJson(word)) // Get Optional from tweet json
                .filter(s-> s.isPresent()) // Filter only present
                .filter(s -> s.get().getIsRetweeted())
                .map(s-> s.get().getRetweetedUserName());


        JavaPairRDD<String, Integer> counts = sentences
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> swapped = counts
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap)
                .sortByKey(false);

        List<Tuple2<Integer, String>> aux = swapped.take(10);
        JavaPairRDD<Integer, String> items = sparkContext.parallelizePairs(aux, 1);

        items.repartition(1).saveAsTextFile(outputDir);

    }

    private static String normalise(String word) {

        return word.trim().toLowerCase();

    }
}
