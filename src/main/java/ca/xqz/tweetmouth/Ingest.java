package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.Tweet;
import ca.xqz.tweetmouth.TweetParser;

import java.io.IOException;

import java.util.List;

import org.elasticsearch.client.transport.TransportClient;

class Ingest {

    public static void main(String[] args) {
        TransportClient client = ESUtil.getTransportClient();
        System.out.println("Successfully created a client");
        TweetParser parser = new TweetParser();

        try {
            List<Tweet> tweetList = parser.getParsedTweets();
            System.out.println(tweetList.get(55));
        } catch (IOException e) {
            System.out.println("IO Exception in parsing tweets");
            throw new RuntimeException(e);
        }

        // client.close();
        System.out.println("Successfully closed a client");
    }
}
