package ca.xqz.tweetmouth;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

public class TweetParser {

    private final static String MESSAGE_KEY = "tweet message";
    private final static String REFERENCES_KEY = "references";
    private final static String HASHTAGS_KEY = "hashtag";

    private BufferedReader input;
    private List<String> orderedLabels;

    public TweetParser(String path) throws Exception {
        this.input = new BufferedReader(new FileReader(path));
        orderedLabels = processTweet(input.readLine()).stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    public TweetParser(List<String> orderedLabels) {
        this.orderedLabels = orderedLabels;
    }

    public List<Map<String, String>> getMappedTweetsFromFile(int num) throws Exception {
        if (input == null) {
            throw new RuntimeException("Trying to get tweets from a non-existent file");
        }
        ArrayList<Map<String, String>> mappedTweets = new ArrayList<>();
        String line = input.readLine();
        for (int i = 0; line != null && i < num; i ++) {
            while (line != null) {
                Map<String, String> tweetMap = getTweetMap(line);
                if (tweetMap != null) {
                    mappedTweets.add(tweetMap);
                    break;
                }
                line = input.readLine();
            }
            line = input.readLine();
        }
        return mappedTweets;
    }

    private Map<String, String> getTweetMap(String tweet) {
        List<String> tokens = processTweet(tweet);
        if (tokens.size() != orderedLabels.size()) {
            System.out.println(tokens.get(0) + " is not properly formatted");
            return null;
        }
        HashMap<String, String> tweetMap = new HashMap<>();
        for (int i = 0; i < orderedLabels.size(); i ++) {
            if (!tokens.get(i).equals("") && !tokens.get(i).equals("null")) {
                tweetMap.put(orderedLabels.get(i), tokens.get(i));
            }
        }
        parseMessage(tweetMap);
        return tweetMap;
    }

    private void parseMessage(HashMap<String, String> tweetMap) {
        String message = tweetMap.get(MESSAGE_KEY);
        List<String> tokens = Arrays.asList(message.split("\\s+"));
        Optional<String> references = tokens.parallelStream()
                .filter(s -> s.startsWith("@") && s.length() > 1)
                .map(s -> s.substring(1).toLowerCase())
                .reduce((s1, s2) -> s1 + " " + s2);
        Optional<String> hashtags = tokens.parallelStream()
                .filter(s -> s.startsWith("#") && s.length() > 1)
                .map(s -> s.substring(1).toLowerCase())
                .reduce((s1, s2) -> s1 + " " + s2);
        references.ifPresent(s -> tweetMap.put(REFERENCES_KEY, s));
        hashtags.ifPresent(s -> tweetMap.put(HASHTAGS_KEY, s));
    }

    private List<String> processTweet(String tweet) {
        List<String> tokenList = Arrays.asList(tweet.split("~"));
        return tokenList.stream()
                .map(String::trim)
                .collect(Collectors.toList());
    }

    @Override
    public void finalize() throws Exception {
        if (input != null) {
            input.close();
        }
    }

    public static void main(String[] args) throws Exception {
        TweetParser parser = new TweetParser("D:\\Programming\\tweetmouth\\TweetsDataset.txt");
        List<Map<String, String>> mappedTweets = parser.getMappedTweetsFromFile(100000);
        for (Map<String, String> map : mappedTweets) {
            for (String key : map.keySet()) {
                System.out.println(key + ": " + map.get(key));
            }
            System.out.println();
        }
    }
}
