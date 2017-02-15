package ca.xqz.tweetmouth;

import java.util.Arrays;
import java.util.List;
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

        try {
            id = Long.parseLong(tokens.get(0));
        } catch (NumberFormatException e) {
            // TODO: Replace with logging
            // System.out.println(tokens.get(0) + " is not properly formatted");
            return null;
        }
        try {
            userId = Long.parseLong(tokens.get(7));
        } catch (NumberFormatException e) {
            // TODO: Replace with logging
            // System.out.println(tokens.get(0) + " is not properly formatted");
            return null;
        }

        String check = tokens.get(1);
        if (!check.equals("null") && !check.equals("")) {
            handle = check;
        }
        check = tokens.get(2);
        if (!check.equals("null") && !check.equals("")) {
            name = check;
        }
        check = tokens.get(3);
        if (!check.equals("null") && !check.equals("")) {
            message = check;
        }
        check = tokens.get(4);
        if (!check.equals("null") && !check.equals("")) {
            createdAt = check;
        }
        check = tokens.get(5);
        if (!check.equals("null") && !check.equals("")) {
            userLocation = check;
        }
        check = tokens.get(6);
        if (!check.equals("null") && !check.equals("")) {
            geoLocation = check;
        }

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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("id: " + id + "\n");
        if (handle != null) {
            sb.append("handle: " + handle + "\n");
        }
        if (name != null) {
            sb.append("name: " + name + "\n");
        }
        if (message != null) {
            sb.append("message: " + message + "\n");
        }
        if (createdAt != null) {
            sb.append("createdAt: " + createdAt + "\n");
        }
        if (userLocation != null) {
            sb.append("userLocation: " + userLocation + "\n");
        }
        if (geoLocation != null) {
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