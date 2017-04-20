package tweetmouth;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class App {
    public static void main( String[] args ) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<Tweet> validTweets = ParseTweet.filterAndParseLine(lines);
        System.out.println("Lines: " + lines.count());
        System.out.println("Valid tweets: " + validTweets.count());
    }
}
