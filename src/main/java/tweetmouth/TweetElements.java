package tweetmouth;

import java.util.List;

public class TweetElements {

    public String cleanedTweet;
    public List<String> hashtags;

    public TweetElements(String cleanedTweet, List<String> hashtags) {
        this.cleanedTweet = cleanedTweet;
        this.hashtags = hashtags;
    }
}
