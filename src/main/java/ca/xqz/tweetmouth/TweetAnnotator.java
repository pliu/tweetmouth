package ca.xqz.tweetmouth;

import edu.stanford.nlp.ling.CoreAnnotation;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class TweetAnnotator implements Annotator {
    private static final String ANNOTATE_STRING = "preproctweet";

    public void annotate(Annotation annotation) {
        /* TODO: idklol */
    }

    public Set<Class<? extends CoreAnnotation>> requirementsSatisfied() {
        return Collections.emptySet();
    }

    public Set<Class<? extends CoreAnnotation>> requires() {
        return Collections.emptySet();
    }

    public static String getProp() {
        return ANNOTATE_STRING;
    }

    public static void setProps(Properties props) {
        props.setProperty("customAnnotatorClass." + ANNOTATE_STRING,
                          "ca.xqz.tweetmouth.TweetAnnotator");
    }
}
