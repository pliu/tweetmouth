package tweetmouth;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TweetPivot {

    public static String FILTERED_PATH = "D:\\Programming\\spark_clustering\\intermediate\\filtered_tweets";
    public static String FEATURES_PATH = "D:\\Programming\\spark_clustering\\intermediate\\features";

    private static int MIN_NUM_TOKENS_PER_TWEET = 6;
    private static int MIN_FEATURE_COUNT_ACROSS_TWEETS = 10;
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
    // TODO: Need a way to filter out non-informative features
    public static JavaRDD<String> parseAndFilterFeatures(JavaPairRDD<Long, TweetElements> tweets, boolean cached,
                                                         boolean save) {
        if (cached) {
            tweets = JavaPairRDD.fromJavaRDD(App.sc.objectFile(FILTERED_PATH));
        }
        JavaRDD<String> features = tweets
                .flatMapToPair(tweet -> {
                    HashSet<String> s = new HashSet<>();
                    for (String hashtag : tweet._2().hashtags) {
                        s.add(hashtag);
                    }
                    List<String> tokens = tweet._2().tokens;
                    for (int i = 0; i <= tokens.size() - 2; i ++) {
                        String gram = Utils.listToString(tokens.subList(i, i + 2));
                        s.add(gram);
                        if (i <= tokens.size() - 3) {
                            gram += " " + tokens.get(i + 2);
                            s.add(gram);
                        }
                    }
                    ArrayList<Tuple2<String, Integer>> a = new ArrayList<>();
                    for (String f : s) {
                        a.add(new Tuple2(f, 1));
                    }
                    return a.iterator();
                })
                .reduceByKey((countA, countB) -> countA + countB)

                // May want to change the threshold to be dynamic
                .filter(feature -> feature._2() >= MIN_FEATURE_COUNT_ACROSS_TWEETS)
                .map(tuple -> tuple._1());
        if (save) {
            features.saveAsObjectFile(FEATURES_PATH);
        }
        return features;
    }

    public static JavaPairRDD<Long, Vector> generateFeatureVectors(JavaPairRDD<Long, TweetElements> tweets) {
        return null;
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
                    }
                    return "";
                })

                // Remove apostrophes
                .map(token -> token.replace("'", ""))

                // Remove leading quotes
                .map(token -> {
                    if (token.startsWith("\"")) {
                        return token.substring(1);
                    }
                    return token;
                })

                // May want to remove trailing emojis here

                // Remove stopwords
                .map(token -> {
                    if (Utils.STOP_WORDS.contains(token)) {
                        return "";
                    }
                    return token;
                })

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

        if (tokens.size() < MIN_NUM_TOKENS_PER_TWEET) {
            return null;
        }
        return new TweetElements(tokens, hashtags);
    }
}
