package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class App {

    private static String VALID_PATH = "D:\\Programming\\spark_clustering\\intermediate\\valid_tweets";
    private static String FILTERED_PATH = "D:\\Programming\\spark_clustering\\intermediate\\filtered_tweets";

    public static void main( String[] args ) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));

        JavaRDD<String> lines = sc.textFile("D:\\Programming\\spark_clustering\\TweetsDataset.txt");

        JavaPairRDD<Long, String> validTweets = ParseTweet.parseAndFilterTweets(lines);
        validTweets.saveAsObjectFile(VALID_PATH);
        // JavaPairRDD<Long, String> validTweets = JavaPairRDD.fromJavaRDD(sc.objectFile(VALID_PATH));

        JavaPairRDD<Long, String> filteredTweets = FeaturePivot.cleanAndFilterTweets(validTweets);
        filteredTweets.saveAsTextFile(FILTERED_PATH);

        System.out.println("Lines: " + lines.count());
        System.out.println("Valid tweets: " + validTweets.count());
        System.out.println("Filtered tweets: " + filteredTweets.count());
    }
}
