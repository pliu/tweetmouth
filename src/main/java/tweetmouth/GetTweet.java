package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Objects;

public class GetTweet {

    public static String VALID_PATH = "D:\\Programming\\spark_clustering\\intermediate\\valid_tweets";

    private final static int NUM_NONDERIVED_FIELDS = 8;

    public static JavaPairRDD<Long, String> getAndFilterTweets(JavaRDD<String> lines, boolean save) {
        JavaPairRDD<Long, String> validTweets = lines
                .map(str -> str.split("~"))
                .map(GetTweet::parsLine)
                .filter(Objects::nonNull)
                .mapToPair(tuple -> tuple);
        if (save) {
            validTweets.saveAsObjectFile(VALID_PATH);
        }
        return validTweets;
    }

    private static Tuple2<Long, String> parsLine(String[] tokens) {
        if (tokens.length != NUM_NONDERIVED_FIELDS) {
            return null;
        }
        long id;
        try {
            id = Long.parseLong(tokens[0]);
        } catch (NumberFormatException e) {
            return null;
        }
        if (tokens[3].equals("null") || tokens[3].equals("")) {
            return null;
        }
        String message = tokens[3];
        return new Tuple2<>(id, message);
    }
}
