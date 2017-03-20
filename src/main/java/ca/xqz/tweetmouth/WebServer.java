package ca.xqz.tweetmouth;

import org.elasticsearch.action.search.SearchResponse;

import java.util.HashMap;
import java.util.Map;

import spark.ModelAndView;
import spark.Spark;
import spark.TemplateEngine;
import spark.TemplateViewRoute;

import spark.template.mustache.MustacheTemplateEngine;

class WebServer {
    private static ESClient client = new ESClient();

    private final static TemplateEngine TEMPLATE = new MustacheTemplateEngine();
    private final static TemplateViewRoute SEARCH = (req, res) -> {
        SearchResponse search_res = client.simpleQuerySearch(req.queryParams("q"));
        Map<String, Object> temp_res = new HashMap<String, Object>();
        temp_res.put("resp", search_res);
        temp_res.put("hits", search_res.getHits());
        temp_res.put("total_hits", search_res.getHits().getTotalHits());
        temp_res.put("suggest", search_res.getSuggest());
        System.out.println(search_res);
        return new ModelAndView(temp_res, "results.html");
    };

    public static void main(String[] args) {
        Spark.staticFileLocation("/public");
        Spark.get("/search", SEARCH, TEMPLATE);
    }

}
