package clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Objects;

public class GetTweet {

    private final static String VALID_PATH = "D:\\Programming\\spark_clustering\\intermediate\\valid_tweets";
    private final static int NUM_NONDERIVED_FIELDS = 8;

    public static JavaPairRDD<Long, String> getAndFilterTweets(JavaRDD<String> lines, boolean cached, boolean save) {
        if (cached) {
            return JavaPairRDD.fromJavaRDD(App.sc.objectFile(VALID_PATH));
        }
        JavaPairRDD<Long, String> validTweets = lines
                .map(str -> str.split("~"))
                .map(GetTweet::parseLine)
                .filter(Objects::nonNull)
                .mapToPair(tuple -> tuple);
        if (save) {
            validTweets.saveAsObjectFile(VALID_PATH);
        }
        return validTweets;
    }

    private static Tuple2<Long, String> parseLine(String[] tokens) {
        if (tokens.length != NUM_NONDERIVED_FIELDS) {
            return null;
        }
        long id;
        try {
            id = Long.parseLong(tokens[0]);
        } catch (NumberFormatException e) {
            return null;
        }
        // A tweet starting with RT means it is a retweet
        if (tokens[3].equals("null") || tokens[3].equals("") || tokens[3].substring(0, 2).toLowerCase()
                .equals("rt")) {
            return null;
        }
        String message = tokens[3];
        return new Tuple2<>(id, message);
    }
}
