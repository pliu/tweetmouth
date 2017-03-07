package ca.xqz.tweetmouth;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        Map<String, Object> temp_res = new HashMap<String, Object>();
        temp_res.put("resp", search_res);
        temp_res.put("hits", search_res.getHits());
        temp_res.put("total_hits", search_res.getHits().getTotalHits());
        temp_res.put("suggest", search_res.getSuggest());
        System.out.println(search_res);
        return new ModelAndView(temp_res, "results.html");
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

    public static void main(String[] args) throws Exception {
        TransportClient client = getTransportClient();
        System.out.println("Successfully created a client");

        // client.close();
        System.out.println("Successfully closed a client");

        Spark.staticFileLocation("/public");
        Spark.get("/search", SEARCH, TEMPLATE);
    }
}
