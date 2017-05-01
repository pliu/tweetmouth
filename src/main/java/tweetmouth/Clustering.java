package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class Clustering {

    public static final void cluster(JavaPairRDD<Long, Vector> vectors, int clusters) {
        BisectingKMeans bkm = new BisectingKMeans().setK(clusters);
        JavaRDD<Vector> v = vectors.map(tuple -> tuple._2());
        BisectingKMeansModel model = bkm.run(v);
        System.out.println(model.computeCost(v));
        for (Vector vec : model.clusterCenters()) {
            System.out.println(vec);
        }
    }
}
