package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.Tweet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TweetParser {

    public static List<Tweet> parseTweetsFromFile(InputStream in) throws IOException {
        Map<String, Integer> tweetFieldMap = new HashMap<String, Integer>();
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        List<Tweet> tweets = new ArrayList<Tweet>();

        String line = bf.readLine();
        if (line == null) {
            return tweets;
        }

        String[] cols = line.split("~", -1);
        for (int i = 0; i < cols.length; i++) {
            tweetFieldMap.put(cols[i], i);
        }

        for (line = bf.readLine(); line != null; line = bf.readLine()) {
            String[] tokens = line.split("~", -1);
            while (tokens.length < 8) {
                line += "\n";
                String tmp = bf.readLine();
                if (tmp == null)
                    return tweets;
                line += tmp;
                tokens = line.split("~", -1);
            }

            String id = tokens[tweetFieldMap.get("Tweet Id")];
            String handle = tokens[tweetFieldMap.get("User Name ")];
            String name = tokens[tweetFieldMap.get(" User ID")];
            String msg = tokens[tweetFieldMap.get("Tweet message")];
            String creation = tokens[tweetFieldMap.get("creation time")];
            String user_loc = tokens[tweetFieldMap.get("User location")];
            String geo_loc = tokens[tweetFieldMap.get("GeoLocation")];
            String user_id = tokens[tweetFieldMap.get("getUser ID")];
            tweets.add(new Tweet(id, handle, name, msg, creation, user_loc, geo_loc, user_id));
        }

        return tweets;
    }
}
