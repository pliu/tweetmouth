package ca.xqz.tweetmouth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class TweetParserFromOracleDb extends TweetParser {

    private Connection conn = null;
    private int startAt = 0;

    public TweetParserFromOracleDb(String url, String username, String password, int startAt) throws Exception {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        conn = DriverManager.getConnection(url, username, password);
        if (conn == null) {
            throw new RuntimeException("Could not create connection to " + url + " as " + username);
        }
        this.startAt = startAt;
    }

    @Override
    public List<Tweet> getParsedTweets(int num) throws Exception {
        return null;
    }

    private static Tweet getParsedTweet() {
        return null;
    }
}
