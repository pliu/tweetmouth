package ca.xqz.tweetmouth;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TweetParser {

    private BufferedReader input;

    public TweetParser(InputStream in) throws Exception {
        this.input = new BufferedReader(new InputStreamReader(in));
    }

    public List<Tweet> getParsedTweets(int num) throws Exception {
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

    public static void main(String[] args) throws Exception {
        TweetParser parser = new TweetParser(new FileInputStream("D:\\Programming\\tweetmouth\\TweetsDataset.txt"));
        List<Tweet> parsedTweets = parser.getParsedTweets(100000);
        for (Tweet tweet: parsedTweets) {
            System.out.println(tweet);
        }
    }
}
