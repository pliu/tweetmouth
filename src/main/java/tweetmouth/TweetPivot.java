package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TweetPivot {

    public static String FILTERED_PATH = "D:\\Programming\\spark_clustering\\intermediate\\filtered_tweets";
    public static String FEATURES_PATH = "D:\\Programming\\spark_clustering\\intermediate\\features";

    private static int MIN_NUM_TOKENS = 6;
    private final static Pattern HANDLE_PATTERN = Pattern.compile("@([a-z0-9_]{1,15})$");
    private final static Pattern TRAILING_PUNCTUATION_PATTERN = Pattern.compile("(.*?)\\p{Punct}*$");

    public static JavaPairRDD<Long, TweetElements> parseAndFilterTweets(JavaPairRDD<Long, String> tweets,
                                                                        boolean cached, boolean save) {
        if (cached) {
            tweets = JavaPairRDD.fromJavaRDD(App.sc.objectFile(GetTweet.VALID_PATH));
        }
        JavaPairRDD<Long, TweetElements> filteredTweets = tweets
                .mapValues(TweetPivot::parseTweet)
                .filter(tuple -> tuple._2() != null);
        if (save) {
            filteredTweets.saveAsObjectFile(FILTERED_PATH);
        }
        return filteredTweets;
    }

    // Generate bi-gram and tri-gram features from cleaned tweets
    public static JavaRDD<String> parseAndFilterFeatures(JavaPairRDD<Long, TweetElements> tweets) {
        return null;
    }

    public static JavaPairRDD<Long, String> generateFeatureVectors(JavaPairRDD<Long, String> tweets) {
        return tweets;
    }

    // Token parsing and cleaning, and tweet filtering
    private static TweetElements parseTweet(String tweet) {
        ArrayList<String> hashtags = new ArrayList<>();
        List<String> dirtyTokens = Arrays.asList(tweet.split("\\s+"));
        List<String> tokens = dirtyTokens.stream()

                // Remove trailing punctuation
                .map(token -> {
                    Matcher m = TRAILING_PUNCTUATION_PATTERN.matcher(token.toLowerCase());
                    if (m.find()) {
                        return m.group(1);
                    } else {
                        return "";
                    }
                })

                // May want to remove trailing emojis here

                // Remove mentions, URLs, and lone #s
                .filter(token -> !(HANDLE_PATTERN.matcher(token).find() || token.startsWith("http") ||
                        (token.startsWith("#") && token.length() == 1) || token.length() == 0))

                // De-#s hashtags, which are added to both tokens and hashtags lists
                .map(token -> {
                    if (token.startsWith("#")) {
                        hashtags.add(token.substring(1));
                        return token.substring(1);
                    }
                    return token;
                })
                .collect(Collectors.toList());

        if (tokens.size() < MIN_NUM_TOKENS) {
            return null;
        }
        return new TweetElements(tokens, hashtags);
    }
}
