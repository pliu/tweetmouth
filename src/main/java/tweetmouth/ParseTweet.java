package tweetmouth;

import org.apache.spark.api.java.JavaRDD;

import java.util.Objects;

public class ParseTweet {

    public static JavaRDD<Tweet> filterAndParseLine(JavaRDD<String> lines) {
        return lines.map(s -> s.split("~"))
                    .map(Tweet::getTweet)
                    .filter(Objects::nonNull);
    }
}
