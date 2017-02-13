package ca.xqz.tweetmouth;

import ca.xqz.tweetmouth.Tweet;
import ca.xqz.tweetmouth.TweetParser;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.io.IOException;

import java.util.List;

import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.transport.client.PreBuiltTransportClient;

import spark.ModelAndView;
import spark.Spark;
import spark.TemplateEngine;
import spark.TemplateViewRoute;

import spark.template.mustache.MustacheTemplateEngine;

class Main {
    private final static int CLIENT_PORT = 9300;
    private final static TemplateEngine TEMPLATE = new MustacheTemplateEngine();
    private final static TemplateViewRoute SEARCH = (req, res) -> {
        TransportClient client = getTransportClient();
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery(req.queryParams("q"));
        SearchResponse search_res = client.prepareSearch().setQuery(qb).get();
        return new ModelAndView(search_res, "results.html");
    };

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

        client.close();
        System.out.println("Successfully closed a client");

        Spark.staticFileLocation("/public");
        Spark.get("/search", SEARCH, TEMPLATE);
    }
}
