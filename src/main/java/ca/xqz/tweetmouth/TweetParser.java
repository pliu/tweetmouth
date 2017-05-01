package ca.xqz.tweetmouth;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TweetParser {

    private BufferedReader input;

    public TweetParser() {
        this.input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    public List<Tweet> getParsedTweets() throws IOException {
        List<Tweet> tweets = new ArrayList<Tweet>();
        do {
            List<Tweet> ofSomeTweets = getParsedTweets(ESClient
                                                       .getDefaultLoadSize());
            if (ofSomeTweets.size() == 0)
                break;
            tweets.addAll(ofSomeTweets);
        } while(true);
        return tweets;
    }

    // TODO: Handle exceptions if we hook this up to the Twitter stream
    public List<Tweet> getParsedTweets(int num) throws IOException {
        ArrayList<Tweet> parsedTweets = new ArrayList<>();
        String line = input.readLine();
        for (int i = 0; line != null && i < num; i ++) {
            while (line != null) {
                Tweet parsedTweet = getParsedTweet(line);
                if (parsedTweet != null) {
                    parsedTweets.add(parsedTweet);
                    break;
                }
                line = input.readLine();
            }
            line = input.readLine();
        }
        return parsedTweets;
    }

    private Tweet getParsedTweet(String tweet) {
        List<String> tokenList = Arrays.asList(tweet.split("~"));
        List<String> tokens = tokenList.stream()
                .map(String::trim)
                .collect(Collectors.toList());
        return Tweet.getTweet(tokens);
    }

    // Simply for testing purposes
    public static void main(String[] args) {
        TweetParser parser = new TweetParser();
        List<Tweet> parsedTweets;
        try {
            parsedTweets = parser.getParsedTweets(100000);
        } catch (IOException e) {
            System.err.println("Lol error:" + e);
            return;
        }
        for (Tweet tweet: parsedTweets) {
            System.out.println(tweet);
        }
    }
}
