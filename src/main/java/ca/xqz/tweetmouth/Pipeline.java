package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.TweetAnnotator;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.List;
import java.util.Properties;

class Pipeline extends StanfordCoreNLP {
    private static final String PROPERTIES = TweetAnnotator.getProp() + ", tokenize, ssplit, parse, sentiment";
    private static final List<Consumer<Properties>> ANNOTATOR_SETTERS = new ArrayList<Consumer<Properties>>();

    static {
        ANNOTATOR_SETTERS.add(TweetAnnotator::setProps);
    };

    public static Pipeline getPipeline() {
        Properties props = new Properties();

        for (Consumer<Properties> annotatorSetter : ANNOTATOR_SETTERS) {
            annotatorSetter.accept(props);
        }

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
