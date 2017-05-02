package clustering;

import java.io.Serializable;
import java.util.List;

public class TweetElements implements Serializable{

    public List<String> tokens;
    public List<String> hashtags;

    public TweetElements(List<String> tokens, List<String> hashtags) {
        this.tokens = tokens;
        this.hashtags = hashtags;
    }
}
