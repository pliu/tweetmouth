package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.Map;

public class App {

    public static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LineCount"));

    public static void main(String[] args) {
        JavaRDD<String> lines = sc.textFile("D:\\Programming\\spark_clustering\\TweetsDataset.txt");

        JavaPairRDD<Long, String> validTweets = GetTweet.getAndFilterTweets(lines, true, true);

        JavaPairRDD<Long, TweetElements> filteredTweets = TweetPivot.parseAndFilterTweets(validTweets, true,
                true);

        JavaPairRDD<String, Integer> features = TweetPivot.parseAndFilterFeatures(filteredTweets, true, true);

        boolean featureVectorsCached = true;
        long numTweets = 0;
        Map<String, Integer> enumeratedFeatures = null;
        Map<String, Integer> documentFeatureCounts = null;
        if (!featureVectorsCached) {
            filteredTweets = filteredTweets.cache();
            features = features.cache();
            numTweets = filteredTweets.count();
            enumeratedFeatures = Utils.enumerate(features.map(tuple -> tuple._1()).collect());
            documentFeatureCounts = Utils.getMap(features.collect());
        }

        JavaPairRDD<Long, Vector> featureVectors = TweetPivot.generateFeatureVectors(filteredTweets,
                enumeratedFeatures, documentFeatureCounts, numTweets, featureVectorsCached, true);

        Clustering.cluster(featureVectors, 5);

        /*System.out.println(validTweets.count());
        System.out.println(numTweets);
        System.out.println(features.count());

        for (Tuple2<String, Integer> t : features.takeSample(false, 100)) {
            System.out.println(t._1() + ": " + t._2());
        }

        for (Map.Entry<String, Integer> e : enumeratedFeatures.entrySet()) {
            System.out.println(e.getKey() + ": " + e.getValue());
        }

        for (Map.Entry<String, Integer> e : documentFeatureCounts.entrySet()) {
            System.out.println(e.getKey() + ": " + e.getValue());
        }

        for (Tuple2<Long, Vector> t : featureVectors.takeSample(false, 10)) {
            System.out.println(t._1() + ": " + t._2().toJson());
        }
        System.out.println(featureVectors.count());*/
    }
}
