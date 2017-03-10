package ca.xqz.tweetmouth;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Tweet {

    public final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final static DateFormat OUTPUT_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT);
    private final static DateFormat INPUT_DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",
            Locale.ENGLISH);
    private final static Pattern GEOLOCATION_PATTERN = Pattern
            .compile(".*latitude=(-?\\d+\\.\\d+), longitude=(-?\\d+\\.\\d+)}");
    private final static int NUM_NONDERIVED_FIELDS = 8;
    private final static Pattern HANDLE_PATTERN = Pattern.compile("@([a-z0-9_]{1,15})$");

    static {
        INPUT_DATE_FORMAT.setLenient(true);
    }

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
            return null;
        }

        long id, userId;
        String handle, name, message, createdAt, userLocation, geoLocation;

        id = longTokenCheck.applyAsLong(tokens.get(0));
        userId = longTokenCheck.applyAsLong(tokens.get(7));
        if (id == -1 || userId == -1) {
            return null;
        }

        handle = stringTokenCheck.apply(tokens.get(1));
        name = stringTokenCheck.apply(tokens.get(2));
        message = stringTokenCheck.apply(tokens.get(3));
        createdAt = stringTokenCheck.apply(tokens.get(4));
        userLocation = stringTokenCheck.apply(tokens.get(5));
        geoLocation = stringTokenCheck.apply(tokens.get(6));

        if (createdAt != null) {
            Date date;
            try {
                date = INPUT_DATE_FORMAT.parse(createdAt);
                createdAt = OUTPUT_DATE_FORMAT.format(date);
            } catch (ParseException e) {
                return null;
            }
        }

        if (geoLocation != null) {
            Matcher m = GEOLOCATION_PATTERN.matcher(geoLocation);
            if (m.find()) {
                geoLocation = m.group(1) + "," + m.group(2);
            }
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

    public long getId() {
        return id;
    }

    private void parseMessage() {
        if (message != null) {
            List<String> tokens = Arrays.asList(message.split("\\s+"));
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
            }
        }
    }

    private static Function<String, String> stringTokenCheck = (token) -> (token.equals("null") || token.equals("")) ?
            null : token.toLowerCase();

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