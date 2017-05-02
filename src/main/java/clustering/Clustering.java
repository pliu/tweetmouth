package clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.function.BiFunction;

// TODO: Currently a bug in mllib of Spark that causes clustering to fail with certain values of k (I think it's any k
// above 7)
public class Clustering {

    private final static BiFunction<Double, Integer, Double> COST_FUNC = (cost, numClusters) -> cost + numClusters*25;
    private final static int STARTING_K = 5;

    public static JavaPairRDD<Integer, Iterable<Long>> cluster(JavaPairRDD<Long, Vector> labelledVectors) {
        JavaRDD<Vector> vectors = labelledVectors.map(Tuple2::_2).cache();
        BisectingKMeansModel model = optimizeModel(vectors);
        return assignToClusters(labelledVectors, model);
    }

    private static JavaPairRDD<Integer, Iterable<Long>> assignToClusters(JavaPairRDD<Long, Vector> labelledVectors,
                                                                          BisectingKMeansModel model) {
        return labelledVectors
                .mapToPair(tuple -> new Tuple2<>(model.predict(tuple._2()), tuple._1()))
                .groupByKey();
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
