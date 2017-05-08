package ca.xqz.tweetmouth;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TweetParserFromStdIn extends TweetParser{

    private BufferedReader input = null;

    public TweetParserFromStdIn() {
        this.input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    // TODO: Handle exceptions if we hook this up to the Twitter stream
    @Override
    public List<Tweet> getParsedTweets(int num) throws Exception {
        ArrayList<Tweet> parsedTweets = new ArrayList<>();
        String line = input.readLine();
        for (int i = 0; line != null && i < num; i++) {
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

    private static Tweet getParsedTweet(String tweet) {
        List<String> tokenList = Arrays.asList(tweet.split("~"));
        List<String> tokens = tokenList.stream()
                .map(String::trim)
                .collect(Collectors.toList());
        return Tweet.getTweet(tokens);
    }

    // Simply for testing purposes
    public static void main(String[] args) {
        TweetParserFromStdIn parser = new TweetParserFromStdIn();
        List<Tweet> parsedTweets;
        try {
            parsedTweets = parser.getParsedTweets(100000);
        } catch (Exception e) {
            System.err.println("Lol error:" + e);
            return;
        }
        for (Tweet tweet : parsedTweets) {
            System.out.println(tweet);
        }
    }
}
