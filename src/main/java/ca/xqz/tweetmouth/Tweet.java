package ca.xqz.tweetmouth;

public class Tweet {
    private long id;
    private String handle;
    private String name;
    private String message;
    private String created_at;
    private String userLocation;
    private String geoLocation;
    private long userId;

    public Tweet(String id, String handle, String name, String message, String created_at,
                 String userLocation, String geoLocation, String userId) {
        try {
            this.id = Long.parseLong(id);
        } catch (NumberFormatException e) {
            this.id = 0;
        }

        this.handle = handle;
        this.name = name;
        this.message = message;
        this.created_at = created_at;
        this.userLocation = userLocation;
        this.geoLocation = geoLocation;
        try {
            this.userId = Long.parseLong(userId);
        } catch (NumberFormatException e) {
            this.userId = 0;
        }
    }


    public String toString() {
        return "\"" + message + "\"	-@" + handle;
    }
}
