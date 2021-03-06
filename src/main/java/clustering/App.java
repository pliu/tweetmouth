package clustering;

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

        boolean featuresCached = true;
        boolean featureVectorsCached = true;

        long numTweets = 0;
        if (!featuresCached || !featureVectorsCached) {
            numTweets = filteredTweets.count();
        }

        JavaPairRDD<String, Integer> features = TweetPivot.parseAndFilterFeatures(filteredTweets, numTweets,
                featuresCached, true);

        // Maybe move these Maps into generateFeatureVectors so they go out of scope after their use and can be GC-ed
        Map<String, Integer> enumeratedFeatures = null;
        Map<String, Integer> documentFeatureCounts = null;
        if (!featureVectorsCached) {
            enumeratedFeatures = Utils.enumerate(features.map(Tuple2::_1).collect());
            documentFeatureCounts = Utils.getMap(features.collect());
        }

        JavaPairRDD<Long, Vector> featureVectors = TweetPivot.generateFeatureVectors(filteredTweets,
                enumeratedFeatures, documentFeatureCounts, numTweets, featureVectorsCached, true);

        JavaPairRDD<Integer, Long> clusters = Clustering.cluster(featureVectors);

        JavaPairRDD<Integer, String> labelledTweets = Clustering.getCorrespondingTweets(featureVectors, validTweets,
                clusters);

        // System.out.println(validTweets.count());
        // System.out.println(filteredTweets.count());
        // System.out.println(features.count());
        // System.out.println(featureVectors.count());
        Map<Integer, Iterable<String>> clusterMap = Utils.getMap(labelledTweets.groupByKey().collect());
        for (Map.Entry<Integer, Iterable<String>> e : clusterMap.entrySet()) {
            System.out.print(e.getKey() + ": ");
            int count = 0;
            for (String l : e.getValue()) {
                count++;
            }
            System.out.println(count);
        }

        /*for (Tuple2<String, Integer> t : features.takeSample(false, 100)) {
            System.out.println(t._1() + ": " + t._2());
        }*/

        /*for (Tuple2<Long, Vector> t : featureVectors.takeSample(false, 10)) {
            System.out.println(t._1() + ": " + t._2().toJson());
        }*/
    }
}
