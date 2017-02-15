package ca.xqz.tweetmouth;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class Tweet {

    private final static int NUM_NONDERIVED_FIELDS = 8;

    private long id;
    private String handle;
    private String name;
    private String message;
    private String createdAt;
    private String userLocation;
    private String geoLocation;
    private long userId;
    private List<String> references = null;
    private List<String> hashtags = null;

    public static Tweet getTweet(List<String> tokens) {
        if (tokens.size() != Tweet.NUM_NONDERIVED_FIELDS) {
            // TODO: Replace with logging
            // System.out.println(tokens.get(0) + " is not properly formatted");
            return null;
        }

        long id, userId;
        String handle = null, name = null, message = null, createdAt = null, userLocation = null, geoLocation = null;

        id = longTokenCheck.applyAsLong(tokens.get(0));
        userId = longTokenCheck.applyAsLong(tokens.get(7));
        if (id == -1 || userId == -1) {
            // TODO: Replace with logging
            // System.out.println(tokens.get(0) + " is not properly formatted");
            return null;
        }

        handle = stringTokenCheck.apply(tokens.get(1));
        name = stringTokenCheck.apply(tokens.get(2));
        message = stringTokenCheck.apply(tokens.get(3));
        createdAt = stringTokenCheck.apply(tokens.get(4));
        userLocation = stringTokenCheck.apply(tokens.get(5));
        geoLocation = stringTokenCheck.apply(tokens.get(6));

        return new Tweet(id, handle, name, message, createdAt, userLocation, geoLocation, userId);
    }

    private Tweet(long id, String handle, String name, String message, String createdAt,
                 String userLocation, String geoLocation, long userId) {
        this.id = id;
        this.handle = handle;
        this.name = name;
        this.message = message;
        this.createdAt = createdAt;
        this.userLocation = userLocation;
        this.geoLocation = geoLocation;
        this.userId = userId;
        parseMessage();
    }

    private void parseMessage() {
        if (message != null) {
            List<String> tokens = Arrays.asList(message.split("\\s+"));
            references = tokens.parallelStream()
                    .filter(s -> s.startsWith("@") && s.length() > 1)
                    .map(s -> s.substring(1).toLowerCase())
                    .collect(Collectors.toList());
            hashtags = tokens.parallelStream()
                    .filter(s -> s.startsWith("#") && s.length() > 1)
                    .map(s -> s.substring(1).toLowerCase())
                    .collect(Collectors.toList());
            if (references.isEmpty()) {
                references = null;
            }
            if (hashtags.isEmpty()) {
                hashtags = null;
            }
        }
    }

    private static Function<String, String> stringTokenCheck = (token) -> (token.equals("null") || token.equals("")) ? "" : token;

    private static ToLongFunction<String> longTokenCheck = (token) -> {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            return -1;
        }
    };

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("id: " + id + "\n");
        if (handle != "") {
            sb.append("handle: " + handle + "\n");
        }
        if (name != "") {
            sb.append("name: " + name + "\n");
        }
        if (message != "") {
            sb.append("message: " + message + "\n");
        }
        if (createdAt != "") {
            sb.append("createdAt: " + createdAt + "\n");
        }
        if (userLocation != "") {
            sb.append("userLocation: " + userLocation + "\n");
        }
        if (geoLocation != "") {
            sb.append("geoLocation: " + geoLocation + "\n");
        }
        if (references != null) {
            sb.append("references:");
            for (String reference : references) {
                sb.append(" " + reference);
            }
            sb.append("\n");
        }
        if (hashtags != null) {
            sb.append("hashtags:");
            for (String hashtag : hashtags) {
                sb.append(" " + hashtag);
            }
            sb.append("\n");
        }
        sb.append("userId: " + userId + "\n");
        return sb.toString();
    }
}