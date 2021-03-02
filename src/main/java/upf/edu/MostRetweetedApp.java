package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

//spark-submit --class upf.edu.MostRetweetedApp --master local[*] target/lab2-4-1.0-SNAPSHOT.jar ./dataset/out/ ./dataset/Eurovision9.json

public class MostRetweetedApp {
    public static void main(String[] args) throws IOException {
        List<String> argsList = Arrays.asList(args);
        String outputDir = argsList.get(0);
        String input = argsList.get(1);

        SparkConf conf = new SparkConf().setAppName("Twitter Filter");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<ExtendedSimplifiedTweet> sentences = sparkContext.textFile(input)
                .filter(sentence -> !sentence.isEmpty())
                .map(word -> ExtendedSimplifiedTweet.fromJson(word)) // Get Optional from tweet json
                .filter(s -> s.isPresent()) // Filter only present
                .filter(s -> s.get().getIsRetweeted())
                .map(s -> s.get());


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
        List<String> topUsers = TenMostRetUsers.map(x -> x._2).take(10);
        List<Tuple2<String, String>> topUsersTweets;

        //Lista de los tweets de los 10 usuarios mas retweeteados
        JavaPairRDD<Tuple2<String, String>, Integer> countsTweets = sentences
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getRetweetedUserName(), x.getRetweetedText()), 1))
                .filter(y -> topUsers.contains(y._1._1))
                .reduceByKey((a, b) -> a + b);


        //Cuantos retweets tiene el tweet más retweeteado de los 10 usuarios mas retweeteados
        JavaPairRDD<String, Integer> maxRetweets = countsTweets
                .mapToPair(s -> new Tuple2<>(s._1._1, s._2))
                .reduceByKey((a, b) -> Math.max(a, b));

        List<Tuple2<String, Integer>> maxRetweetsUser = maxRetweets.collect();

        //Busqueda final
        JavaPairRDD<Integer, Tuple2<String, String>> result = countsTweets
                .mapToPair(x -> new Tuple2<>(x._2, new Tuple2<>(x._1._1, x._1._2)))
                .filter(y -> topUsers.contains(y._2._1))
                .filter(z -> maxRetweetsUser.contains(new Tuple2<>(z._2._1, z._1)))
                .sortByKey(false);

        result.repartition(1).saveAsTextFile(outputDir);
    }
}
