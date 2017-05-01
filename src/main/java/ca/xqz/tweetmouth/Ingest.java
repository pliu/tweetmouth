package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.ESClient;
import ca.xqz.tweetmouth.Tweet;
import ca.xqz.tweetmouth.TweetParser;

import edu.stanford.nlp.pipeline.Annotation;

import java.io.IOException;

import java.util.List;
import java.util.stream.Stream;

class Ingest {
    private static int PARSED_TWEET_BUF_SIZE = 100;

    private static boolean ingestTweets(TweetParser parser, Pipeline pipeline, ESClient client) {
        List<Tweet> tweets;
        boolean moreTweets;

        try {
            tweets = parser.getParsedTweets(PARSED_TWEET_BUF_SIZE);
        } catch (IOException e) {
            System.out.println("IO Exception in parsing tweets");
            throw new RuntimeException(e);
        }
        moreTweets = tweets.size() == PARSED_TWEET_BUF_SIZE;

        Stream<Tweet> annotations = tweets.parallelStream().map(
            tweet -> {
                Annotation a = pipeline.annotate(tweet);
                try {
                    TweetJson.addAnnotation(tweet, a);
                } catch (IOException e) {
                }
                return tweet;
            });

        client.loadTweets(annotations);
        return moreTweets;
    }

    public static void main(String[] args) {
        TweetParser parser = new TweetParser();
        ESClient client = new ESClient();
        final Pipeline pipeline = Pipeline.getPipeline();

        while (ingestTweets(parser, pipeline, client));
        client.close();
    }
}
