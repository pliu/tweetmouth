package ca.xqz.tweetmouth;

import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.io.StringOutputStream;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.JSONOutputter;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.trees.TreePrint;
import edu.stanford.nlp.util.CoreMap;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.List;

class TweetJson extends JSONOutputter {

    /*private final Function<CoreLabel, Consumer<Writer>> token_map = token -> (Consumer<Writer>) (Writer json) -> {
        json.set("index", token.index());
        json.set("word", token.word());
        json.set("originalText", token.originalText());
        json.set("lemma", token.lemma());
        json.set("pos", token.tag());

        // Do we care about some weird time stuff?
    };

    private final Function<CoreMap, Consumer<Writer>> sentence_map = sentence -> (Consumer<Writer>) (Writer json) -> {
        json.set("id", sentence.get(CoreAnnotations.SentenceIDAnnotation.class));
        json.set("index", sentence.get(CoreAnnotations.SentenceIndexAnnotation.class));

        // TODO: Parse Tree Print

        // Dependency trees - do we care ?

        Tree sentimentTree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        if (sentimentTree != null) {
            json.set("sentimentValue", Integer.toString(RNNCoreAnnotations.getPredictedClass(sentimentTree)));
            json.set("sentiment", sentence.get(SentimentCoreAnnotations.SentimentClass.class).replaceAll(" ", ""));
        }

        // Anything else we care about?

        List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);
        if (tokens != null) {
            json.set("tokens", tokens.stream().map(token_map));
        }
    };

    @Override
    public void print(Annotation doc, OutputStream os, Options o) throws IOException {
        PrintWriter pw = new PrintWriter(IOUtils.encodedOutputStreamWriter(os, o.encoding));
        JSONWriter jw = new JSONWriter(pw, o);

        jw.object(
            json -> {
                json.set("docId", doc.get(CoreAnnotations.DocIDAnnotation.class));
                json.set("docDate", doc.get(CoreAnnotations.DocDateAnnotation.class));
                json.set("author", doc.get(CoreAnnotations.AuthorAnnotation.class));
                json.set("location", doc.get(CoreAnnotations.LocationAnnotation.class));

                if (o.includeText)
                    json.set("text", doc.get(CoreAnnotations.TextAnnotation.class));

                List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);

                // Add sentences
                if (sentences != null) {
                    json.set("sentences", sentences.stream().map(sentence_map));
                }
            });
        jw.flush();
    }

    public static String toJson(Annotation a, Pipeline p) throws IOException {
        Options o = getOptions(p);
        o.pretty = false; // Collapse whitespace
        o.includeText = true; // Include original "text" - content of the tweet
        StringOutputStream s = new StringOutputStream();
        new TweetJson().print(a, s, o);
        return s.toString();
    }*/

    public static void addAnnotation(Tweet t, Annotation a) throws IOException {
        List<CoreMap> sentences = a.get(CoreAnnotations.SentencesAnnotation.class);
        Tree sentimentTree = sentences.get(0).get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        if (sentimentTree != null) {
            t.setSentimentValue(RNNCoreAnnotations.getPredictedClass(sentimentTree));
            t.setSentiment(sentences.get(0).get(SentimentCoreAnnotations.SentimentClass.class).replaceAll(" ", ""));
        }
    }

}
