package clustering;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TweetPivot {

    private final static String FILTERED_PATH = "D:\\Programming\\spark_clustering\\intermediate\\filtered_tweets";
    private final static String FEATURES_PATH = "D:\\Programming\\spark_clustering\\intermediate\\features";
    private final static String VECTOR_PATH = "D:\\Programming\\spark_clustering\\intermediate\\vectors";
    private final static int MIN_NUM_TOKENS_PER_TWEET = 6;
    private final static int MIN_FEATURES_PER_TWEET = 3;
    private final static Function<Long, Long> MIN_FEATURE_COUNT_ACROSS_TWEETS_FUNC = numTweets ->
            (long)Math.max(10L, numTweets * 0.0005);
    private final static Pattern HANDLE_PATTERN = Pattern.compile("@([a-z0-9_]{1,15})$");
    private final static Pattern TRAILING_PUNCTUATION_PATTERN = Pattern.compile("(.*?)\\p{Punct}*$");
    private final static Pattern LEADING_PUNCTUATION_PATTERN = Pattern.compile("^[\\p{Punct}&&[^#]]*(.*)");

    public static JavaPairRDD<Long, TweetElements> parseAndFilterTweets(JavaPairRDD<Long, String> tweets,
                                                                        boolean cached, boolean save) {
        if (cached) {
            return JavaPairRDD.fromJavaRDD(App.sc.objectFile(FILTERED_PATH));
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
    public static JavaPairRDD<String, Integer> parseAndFilterFeatures(JavaPairRDD<Long, TweetElements> tweets,
                                                                      long numTweets, boolean cached, boolean save) {
        if (cached) {
            return JavaPairRDD.fromJavaRDD(App.sc.objectFile(FEATURES_PATH));
        }
        long MIN_FEATURE_COUNT_ACROSS_TWEETS = MIN_FEATURE_COUNT_ACROSS_TWEETS_FUNC.apply(numTweets);
        JavaPairRDD<String, Integer> features = tweets
                .flatMapToPair(tweet -> {
                    Set<String> s = getFeatures(tweet);
                    ArrayList<Tuple2<String, Integer>> a = new ArrayList<>();
                    for (String f : s) {
                        a.add(new Tuple2<>(f, 1));
                    }
                    return a.iterator();
                })
                .reduceByKey((countA, countB) -> countA + countB)
                .filter(feature -> feature._2() >= MIN_FEATURE_COUNT_ACROSS_TWEETS);
        if (save) {
            features.saveAsObjectFile(FEATURES_PATH);
        }
        return features;
    }

    public static JavaPairRDD<Long, Vector> generateFeatureVectors(JavaPairRDD<Long, TweetElements> tweets,
                                                                   Map<String, Integer> enumeratedFeatures,
                                                                   Map<String, Integer> documentFeatureCounts,
                                                                   long numTweets, boolean cached, boolean save) {
        if (cached) {
            return JavaPairRDD.fromJavaRDD(App.sc.objectFile(VECTOR_PATH));
        }
        JavaPairRDD<Long, Vector> vectors = tweets
                .mapToPair(tweet -> {
                    Set<String> s = getFeatures(tweet);
                    ArrayList<Tuple2<Integer, Double>> tuples = new ArrayList<>();
                    double total = 0;
                    for (String f : s) {
                        Integer index = enumeratedFeatures.get(f);
                        if (index == null) {
                            continue;
                        }
                        double value = 1.0 / (Math.log(numTweets / documentFeatureCounts.get(f)));
                        tuples.add(new Tuple2<>(index, value));
                        total += value;
                    }
                    if (tuples.size() < MIN_FEATURES_PER_TWEET) {
                        return new Tuple2<>(tweet._1(), null);
                    }
                    ArrayList<Tuple2<Integer, Double>> scaledTuples = new ArrayList<>();
                    double denominator = Math.sqrt(total);
                    for (Tuple2<Integer, Double> tuple : tuples) {
                        scaledTuples.add(new Tuple2<>(tuple._1(), tuple._2()/denominator));
                    }
                    return new Tuple2<>(tweet._1(), Vectors.sparse(enumeratedFeatures.size(), scaledTuples));
                })
                .filter(tuple -> tuple._2() != null);
        if (save) {
            vectors.saveAsObjectFile(VECTOR_PATH);
        }
        return vectors;
    }

    // Token parsing and cleaning, and tweet filtering
    private static TweetElements parseTweet(String tweet) {
        ArrayList<String> hashtags = new ArrayList<>();
        List<String> dirtyTokens = Arrays.asList(tweet.split("\\s+"));
        List<String> tokens = dirtyTokens.stream()

                // Remove trailing punctuation and lowercases the token for subsequent steps
                .map(token -> {
                    Matcher m = TRAILING_PUNCTUATION_PATTERN.matcher(token.toLowerCase());
                    if (m.find()) {
                        return m.group(1);
                    }
                    return "";
                })

                // Remove apostrophes
                .map(token -> token.replace("'", ""))

                // Remove leading punctuation except #s
                .map(token -> {
                    Matcher m = LEADING_PUNCTUATION_PATTERN.matcher(token);
                    if (m.find()) {
                        return m.group(1);
                    }
                    return "";
                })

                // TODO: Remove emojis here

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

    // Could change Set to be a Map of counts if a tweet can contribute more than one of any feature
    private static Set<String> getFeatures(Tuple2<Long, TweetElements> tweet) {
        Set<String> s = new HashSet<>();
        // s.addAll(tweet._2().hashtags);  // Using hashtags as features results in a lot of features such as "anger"
        List<String> tokens = tweet._2().tokens;
        for (int i = 0; i <= tokens.size() - 2; i++) {
            String gram = Utils.listToString(tokens.subList(i, i + 2));
            s.add(gram);
            if (i <= tokens.size() - 3) {
                gram += " " + tokens.get(i + 2);
                s.add(gram);
            }
        }
        return s;
    }
}
