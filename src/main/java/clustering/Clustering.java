package clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

// TODO: Currently a bug in mllib of Spark that causes clustering to fail with certain values of k (I think it's any k
// above 7)
public class Clustering {

    private final static BiFunction<Double, Integer, Double> COST_FUNC = (cost, numClusters) -> cost + numClusters*25;
    private final static int STARTING_K = 5;

    public static JavaPairRDD<Integer, Long> cluster(JavaPairRDD<Long, Vector> vectors) {
        JavaRDD<Vector> projectedVectors = vectors.map(Tuple2::_2).cache();
        BisectingKMeansModel model = optimizeModel(projectedVectors);
        return vectors.mapToPair(tuple -> new Tuple2<>(model.predict(tuple._2()), tuple._1()));
    }

    // May want to split out the filter step so that the Set is in a separate scope and can be GC-ed after being used in
    // the filtering step
    public static JavaPairRDD<Integer, String> getCorrespondingTweets(JavaPairRDD<Long, Vector> vectors,
                                                                   JavaPairRDD<Long, String> validTweets,
                                                                   JavaPairRDD<Integer, Long> clusters) {
        Set<Long> tweetIds = new HashSet<>(vectors.map(Tuple2::_1).collect());
        Map<Long, String> tweetMap = Utils.getMap(validTweets.filter(tuple -> tweetIds.contains(tuple._1())).collect());
        return clusters.mapToPair(tuple -> new Tuple2<>(tuple._1(), tweetMap.get(tuple._2())));
    }

    private static BisectingKMeansModel optimizeModel(JavaRDD<Vector> vectors) {
        int k = STARTING_K;
        double lowestCost = Double.MAX_VALUE;
        BisectingKMeansModel bestModel = null;
        while(true) {
            BisectingKMeansModel model = getModel(vectors, k);
            double cost = COST_FUNC.apply(model.computeCost(vectors), k);
            System.out.println(cost);
            if (cost < lowestCost) {
                lowestCost = cost;
                bestModel = model;
            } else {
                 break;
            }
            k++;
        }
        return bestModel;
    }

    private static BisectingKMeansModel getModel(JavaRDD<Vector> vectors, int clusters) {
        BisectingKMeans bkm = new BisectingKMeans().setK(clusters);
        BisectingKMeansModel model = bkm.run(vectors);
        return model;
    }
}
