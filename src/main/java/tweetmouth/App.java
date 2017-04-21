package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class App {

    public static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));

    private static boolean SAVE_VALID = true;
    private static boolean SAVE_FILTERED = true;

    public static void main(String[] args) {
        JavaRDD<String> lines = sc.textFile("D:\\Programming\\spark_clustering\\TweetsDataset.txt");

        JavaPairRDD<Long, String> validTweets = GetTweet.getAndFilterTweets(lines, SAVE_VALID);

        JavaPairRDD<Long, TweetElements> filteredTweets = TweetPivot.parseAndFilterTweets(validTweets, !SAVE_VALID,
                SAVE_FILTERED);

    }
}
