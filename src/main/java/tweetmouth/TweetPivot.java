package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;

public class TweetPivot {

    public static String FILTERED_PATH = "D:\\Programming\\spark_clustering\\intermediate\\filtered_tweets";
    public static String FEATURES_PATH = "D:\\Programming\\spark_clustering\\intermediate\\features";

    // Generate bi-gram and tri-gram features from cleaned tweets
    // Treat hashtags as individual features
    // Aggregate features
    public static JavaPairRDD<Long, TweetElements> parseAndFilterTweets(JavaPairRDD<Long, String> tweets,
                                                                        boolean cached, boolean save) {
        if (cached) {
            tweets = JavaPairRDD.fromJavaRDD(App.sc.objectFile(GetTweet.VALID_PATH));
        }
        JavaPairRDD<Long, TweetElements> filteredTweets = tweets
                .mapValues(TweetPivot::parseTweet)
                .filter(tuple -> tuple._2() != null);
        if (save) {
            filteredTweets.saveAsObjectFile(FILTERED_PATH);
        }
        return filteredTweets;
    }

    public static JavaRDD<String> parseAndFilterFeatures(JavaPairRDD<Long, TweetElements> tweets) {
        return null;
    }

    public static JavaPairRDD<Long, String> generateFeatureVectors(JavaPairRDD<Long, String> tweets) {
        return tweets;
    }

    private static TweetElements parseTweet(String tweet) {
        ArrayList<String> hashtags = new ArrayList<>();
        ArrayList<String> tokens = new ArrayList<>();
    }
}
