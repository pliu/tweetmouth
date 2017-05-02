package ca.xqz.tweetmouth;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import spark.ModelAndView;
import spark.Spark;
import spark.TemplateEngine;
import spark.TemplateViewRoute;
import spark.template.mustache.MustacheTemplateEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class WebServer {
    private static ESClient client = new ESClient("cpserver.eastus.cloudapp.azure.com", 5000, "tweet_index", "tweet");

    private final static TemplateEngine TEMPLATE = new MustacheTemplateEngine();
    private final static TemplateViewRoute SEARCH = (req, res) -> {
        SearchResponse search_res = client.query(req.queryParams("q"));
        Map<String, Object> temp_res = new HashMap<String, Object>();
        List<String> hits = new ArrayList<String>();
        for (SearchHit hit : search_res.getHits()) {
            Map m = hit.sourceAsMap();
            if (m == null)
                continue;
            hits.add("{\"id\":\"" + m.get("id") + "\", \"handle\":\"" + m.get("handle") + "\"}");
        }
        temp_res.put("resp", search_res);
        temp_res.put("hits", hits);
        temp_res.put("total_hits", search_res.getHits().getTotalHits());
        temp_res.put("suggest", search_res.getSuggest());
        return new ModelAndView(temp_res, "results.html");
    };

    public static void main(String[] args) {
        Spark.staticFileLocation("/public");
        Spark.before((req, resp) -> {
            resp.header("Access-Control-Allow-Origin", "*");
        });
        Spark.get("/search", SEARCH, TEMPLATE);
    }

}
