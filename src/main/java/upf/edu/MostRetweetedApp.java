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

        JavaRDD<ExtendedSimplifiedTweet> sentences = sparkContext.textFile(input)
                .filter(sentence -> !sentence.isEmpty())
                .map(word -> ExtendedSimplifiedTweet.fromJson(word)) // Get Optional from tweet json
                .filter(s-> s.isPresent()) // Filter only present
                .filter(s -> s.get().getIsRetweeted())
                .map(s-> s.get());


        //Usuarios ordenador de mas a menos retweets
        JavaPairRDD<String, Integer> usersCounts = sentences
                .map(s -> s.getRetweetedUserName())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> swapped1 = usersCounts
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap)
                .sortByKey(false);

        //10 tweets con mas retweets
        List<Tuple2<Integer, String>> aux = swapped1.take(10);
        JavaPairRDD<Integer, String> TenMostRetUsers = sparkContext.parallelizePairs(aux, 1);
        List<String> topUsers = TenMostRetUsers.map(x->x._2).take(10);
        List<Tuple2<String, String>> topUsersTweets;

        //Lista de los tweets de los 10 usuarios mas retweeteados
        JavaPairRDD<Tuple2<String, String>, Integer> countsTweets = sentences
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getRetweetedUserName(), x.getRetweetedText()), 1))
                .filter(y -> topUsers.contains(y._1._1))
                .reduceByKey((a, b) -> a + b);


        //Cuantos retweets tiene el tweet más retweeteado de los 10 usuarios mas retweeteados
        JavaPairRDD<Integer, String> swapped2 = countsTweets
                .mapToPair(s-> new Tuple2<>(s._2, s._1._1))
                .sortByKey(Math::max)
                .sortByKey(false);

        swapped2.repartition(1).saveAsTextFile(outputDir);
    }

    private static String normalise(String word) {

        return word.trim().toLowerCase();

    }
}
