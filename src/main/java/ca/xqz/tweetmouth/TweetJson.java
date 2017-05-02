package ca.xqz.tweetmouth;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.JSONOutputter;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.IOException;
import java.util.List;

class TweetJson extends JSONOutputter {

    public static void addAnnotation(Tweet t, Annotation a) throws IOException {
        List<CoreMap> sentences = a.get(CoreAnnotations.SentencesAnnotation.class);
        Tree sentimentTree = sentences.get(0).get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        if (sentimentTree != null) {
            t.setSentimentValue(RNNCoreAnnotations.getPredictedClass(sentimentTree));
            t.setSentiment(sentences.get(0).get(SentimentCoreAnnotations.SentimentClass.class).replaceAll(" ", ""));
        }
    }

}
