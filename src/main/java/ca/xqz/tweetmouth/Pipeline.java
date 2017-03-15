package ca.xqz.tweetmouth;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

class Pipeline extends StanfordCoreNLP {
    private static final String PROPERTIES = "tokenize, pos, lemma, sentiment";

    public static Pipeline getPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", PROPERTIES);
        return new Pipeline(props);
    }

    public Pipeline(Properties props) {
        super(props);
    }

    public void annotate(Tweet tweet) {
        Annotation annotation = new Annotation(tweet.getMessage());
        annotate(annotation);
    }

}
