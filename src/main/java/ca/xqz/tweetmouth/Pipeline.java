package ca.xqz.tweetmouth;


import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

class Pipeline extends StanfordCoreNLP {
    private static final String PROPERTIES = "tokenize, ssplit, parse, sentiment";

    public static Pipeline getPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", PROPERTIES);
        return new Pipeline(props);
    }

    public Pipeline(Properties props) {
        super(props);
    }

    // Do some pre-processing on the string before sentiment analysis
    public Annotation annotate(Tweet tweet) {
        // Treat the entire tweet as one sentence by removing punctuation
        String message = tweet.getMessage().replaceAll("\\p{Punct}", "");
        Annotation annotation = new Annotation(message);
        annotate(annotation);
        return annotation;
    }

}
