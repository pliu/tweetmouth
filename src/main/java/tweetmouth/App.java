package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.List;

public class App {

    public static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));

    public static void main(String[] args) {
        JavaRDD<String> lines = sc.textFile("D:\\Programming\\spark_clustering\\TweetsDataset.txt");

        JavaPairRDD<Long, String> validTweets = GetTweet.getAndFilterTweets(lines, false);

        JavaPairRDD<Long, TweetElements> filteredTweets = TweetPivot.parseAndFilterTweets(validTweets, true,
                false).cache();

        JavaRDD<String> features = TweetPivot.parseAndFilterFeatures(filteredTweets, false, false);

        /*List<Tuple2<Long, TweetElements>> tuples = filteredTweets.collect();
        for (int i = 0; i < 100; i ++) {
            System.out.print(tuples.get(i)._1() + ": ");
            for (String token : tuples.get(i)._2().tokens) {
                System.out.print(token + " ");
            }
            System.out.println();
        }*/

        List<String> tuples = features.collect();
        for (int i = 0; i < 100; i ++) {
            System.out.println(tuples.get(i));
        }
        System.out.println("Features: " + tuples.size());
    }
}
