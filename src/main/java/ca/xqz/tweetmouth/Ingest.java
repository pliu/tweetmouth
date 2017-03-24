package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.ESClient;
import ca.xqz.tweetmouth.Tweet;
import ca.xqz.tweetmouth.TweetParser;

import edu.stanford.nlp.pipeline.Annotation;

import java.io.IOException;

import java.util.List;
import java.util.stream.Stream;

class Ingest {

    public static void main(String[] args) {
        TweetParser parser = new TweetParser();
        ESClient client = new ESClient();
        final Pipeline pipeline = Pipeline.getPipeline();

        List<Tweet> tweets;
        try {
            tweets = parser.getParsedTweets(100);
        } catch (IOException e) {
            System.out.println("IO Exception in parsing tweets");
            throw new RuntimeException(e);
        }

        Stream<String> annotations = tweets.stream().map(
            tweet -> {
                Annotation a = pipeline.annotate(tweet);
                String s;
                try {
                    s = TweetJson.toJson(a, pipeline);
                } catch (IOException e) {
                    s = null;
                }
                return s;
            });

        client.loadTweets(annotations);
        client.close();
    }
}
