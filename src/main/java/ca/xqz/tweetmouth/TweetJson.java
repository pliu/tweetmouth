package ca.xqz.tweetmouth;

import com.google.gson.Gson;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.JSONOutputter;

import java.io.IOException;

class TweetJson {

    private static Gson gson = new Gson();

    public static String toJson(Tweet tweet) {
        return gson.toJson(tweet);
    }

    public static String toJson(Annotation a) throws IOException {
        return JSONOutputter.jsonPrint(a);
    }

}
