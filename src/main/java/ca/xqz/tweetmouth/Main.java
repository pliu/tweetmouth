package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.Tweet;
import ca.xqz.tweetmouth.TweetParser;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.io.IOException;

import java.util.List;

import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.transport.client.PreBuiltTransportClient;

import spark.Spark;
import spark.Route;
import spark.TemplateEngine;

import spark.template.mustache.MustacheTemplateEngine;

class Main {
    private final static int CLIENT_PORT = 9300;
    private final static TemplateEngine TEMPLATE = new MustacheTemplateEngine();

    private static TransportClient getTransportClient() {
        InetAddress localhost;
        try {
            localhost = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            System.out.println("Unknown localhost");
            throw new RuntimeException(e);
        }

        return new PreBuiltTransportClient(Settings.EMPTY)
            .addTransportAddress(new InetSocketTransportAddress(localhost, CLIENT_PORT));
    }

    public static void main(String[] args) {
        TransportClient client = getTransportClient();
        System.out.println("Successfully created a client");

        try {
            List<Tweet> tweetList = TweetParser.parseTweetsFromFile(System.in);
            System.out.println(tweetList.get(55));
        } catch (IOException e) {
            System.out.println("IO Exception in parsing tweets");
            throw new RuntimeException(e);
        }

        Spark.staticFileLocation("/public");
        Spark.init();

        client.close();
        System.out.println("Successfully closed a client");
    }
}
