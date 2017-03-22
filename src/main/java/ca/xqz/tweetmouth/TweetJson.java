package ca.xqz.tweetmouth;

import com.google.gson.Gson;

import edu.stanford.nlp.io.StringOutputStream;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.JSONOutputter;

import java.io.IOException;

class TweetJson extends JSONOutputter {

    private static Gson gson = new Gson();

    public static String toJson(Tweet tweet) {
        return gson.toJson(tweet);
    }

    public static String toJson(Annotation a, Pipeline p) throws IOException {
        Options o = getOptions(p);
        StringOutputStream s = new StringOutputStream();
        jsonPrint(a, s, o);
        return s.toString();
    }

}
