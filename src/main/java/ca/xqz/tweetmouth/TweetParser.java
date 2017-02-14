package ca.xqz.tweetmouth;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

public class TweetParser {

    private BufferedReader input;
    private List<String> orderedLabels;

    public TweetParser(String path) throws Exception {
        this.input = new BufferedReader(new FileReader(path));
        orderedLabels = processTweet(input.readLine());
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
            tweetMap.put(orderedLabels.get(i), tokens.get(i));
        }
        return tweetMap;
    }

    private List<String> processTweet(String tweet) {
        List<String> tokenList = Arrays.asList(tweet.split("~"));
        List<String> processedTokens = tokenList.stream().map(s -> s.trim()).collect(Collectors.toList());
        return processedTokens;
    }

    @Override
    public void finalize() throws Exception {
        if (input != null) {
            input.close();
        }
    }

    public static void main(String[] args) throws Exception {
        TweetParser parser = new TweetParser("D:\\Programming\\tweetmouth\\TweetsDataset.txt");
        for (String label : parser.orderedLabels) {
            System.out.println(label);
        }
        List<Map<String, String>> mappedTweets = parser.getMappedTweetsFromFile(100);
        for (Map<String, String> map : mappedTweets) {
            for (String key : map.keySet()) {
                System.out.print(key + ": " + map.get(key) + "\t");
            }
            System.out.println();
        }
    }
}
