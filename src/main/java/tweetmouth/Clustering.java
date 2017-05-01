package tweetmouth;

import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.linalg.Vector;

public class Clustering {

    public static final Vector[] cluster(int clusters) {
        BisectingKMeans bkm = new BisectingKMeans().setK(clusters);
        return null;
    }
}
