package tweetmouth;

import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tweet {

    private final static int NUM_NONDERIVED_FIELDS = 8;
    private final static Pattern HANDLE_PATTERN = Pattern.compile("@([a-z0-9_]{1,15})$");

    public long id;
    public String message;

    public static Tweet getTweet(String[] tokens) {
        if (tokens.length != Tweet.NUM_NONDERIVED_FIELDS) {
            return null;
        }

        long id;
        String message;

        id = longTokenCheck.applyAsLong(tokens[0]);
        if (id == -1) {
            return null;
        }

        message = stringTokenCheck.apply(tokens[3]);
        if (message == null) {
            return null;
        }

        return new Tweet(id, message);
    }

    private Tweet(long id, String message) {
        this.id = id;
        this.message = message;
        cleanAndParseMessage();
    }

    private void cleanAndParseMessage() {
        /*List<String> tokens = Arrays.asList(message.split("\\s+"));
        references = tokens.parallelStream()
                .filter(s -> HANDLE_PATTERN.matcher(s).find())
                .map(s -> s.substring(1))
                .collect(Collectors.toList());
        hashtags = tokens.parallelStream()
                .filter(s -> s.startsWith("#") && s.length() > 1)
                .map(s -> s.substring(1))
                .collect(Collectors.toList());
        if (references.isEmpty()) {
            references = null;
        }
        if (hashtags.isEmpty()) {
            hashtags = null;
        }*/
    }

    private static Function<String, String> stringTokenCheck = (token) -> (token.equals("null") || token.equals("")) ?
            null : token;

    private static ToLongFunction<String> longTokenCheck = (token) -> {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            return -1;
        }
    };
}
