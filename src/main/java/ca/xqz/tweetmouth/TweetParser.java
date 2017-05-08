package ca.xqz.tweetmouth;

import java.util.List;

public abstract class TweetParser {

    public abstract List<Tweet> getParsedTweets(int num) throws Exception;
}
