package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;

public class FeaturePivot {

    // Generate bi-gram and tri-gram features from cleaned tweets
    // Treat hashtags as individual features
    // Aggregate features
    public static JavaPairRDD<Long, String> cleanAndFilterTweets(JavaPairRDD<Long, String> tweets) {
        return tweets;
    }

    public static JavaPairRDD<Long, String> generateFeatureVectors(JavaPairRDD<Long, String> tweets) {
        return tweets;
    }
}
