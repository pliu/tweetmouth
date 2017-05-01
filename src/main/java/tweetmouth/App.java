package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;

import java.util.Map;

public class App {

    public static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));

    public static void main(String[] args) {
        JavaRDD<String> lines = sc.textFile("D:\\Programming\\spark_clustering\\TweetsDataset.txt");

        JavaPairRDD<Long, String> validTweets = GetTweet.getAndFilterTweets(lines, true, true);

        JavaPairRDD<Long, TweetElements> filteredTweets = TweetPivot.parseAndFilterTweets(validTweets, true,
                true);

        JavaPairRDD<String, Integer> features = TweetPivot.parseAndFilterFeatures(filteredTweets, true, true);

        boolean featureVectorsCached = false;
        long numTweets = 0;
        Map<String, Integer> enumeratedFeatures = null;
        Map<String, Integer> documentFeatureCounts = null;
        if (!featureVectorsCached) {
            filteredTweets = filteredTweets.cache();
            features = features.cache();
            numTweets = filteredTweets.count();
            enumeratedFeatures = Utils.enumerate(features.map(tuple -> tuple._1()).collect());
            documentFeatureCounts = features.collectAsMap();
        }

        JavaPairRDD<Long, Vector> featureVectors = TweetPivot.generateFeatureVectors(filteredTweets,
                enumeratedFeatures, documentFeatureCounts, numTweets, featureVectorsCached, false);

        for (String s : enumeratedFeatures.keySet()) {
            System.out.println(s + " " + enumeratedFeatures.get(s));
        }
        System.out.println("Features: " + enumeratedFeatures.size());
    }
}
