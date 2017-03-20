package ca.xqz.tweetmouth;

import com.google.gson.Gson;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.List;

class ESClient {

    private final static String DEFAULT_HOST = "127.0.0.1";
    private final static int DEFAULT_PORT = 9300;
    private final static int DEFAULT_LOAD_SIZE = 1000;

    private TransportClient client;
    private Gson gson;
    private String index;
    private String type;

    public ESClient() {
        client = getTransportClient(DEFAULT_HOST, DEFAULT_PORT);
        index = "test_index";
        type = "tweet";
        _baseConstruction();
    }

    public ESClient(String host, int port, String index, String type) {
        client = getTransportClient(host, port);
        this.index = index;
        this.type = type;
        _baseConstruction();
    }

    private TransportClient getTransportClient(String host, int port) {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return new PreBuiltTransportClient(Settings.EMPTY)
            .addTransportAddress(new InetSocketTransportAddress(inetAddr, port));
    }

    private void _baseConstruction() {
        gson = new Gson();
        IndicesExistsResponse resp = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (resp.isExists())
            return;

        System.out.println("Index doesn't exist; creating it");
        client.admin().indices().prepareCreate(index)
            .addMapping(type, "{\n" +
                        "    \"" + type + "\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"createdAt\": {\n" +
                        "          \"type\": \"date\",\n" +
                        "          \"format\": \"" + Tweet.DATE_FORMAT + "\"" +
                        "        },\n" +
                        "        \"geoLocation\": {\n" +
                        "          \"type\": \"geo_point\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }")
            .get();
    }

    // TODO: Add buffering if we hook this up to the Twitter stream
    public void loadTweets(List<Tweet> tweets) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tweet tweet : tweets) {
            bulkRequest.add(new IndexRequest(index, type, Long.toString(tweet.getId()))
                            .source(gson.toJson(tweet)));
        }

        int count = 0;
        BulkResponse resp = bulkRequest.get();
        if (resp.hasFailures()) {
            for (BulkItemResponse r : resp.getItems()) {
                if (r.isFailed()) {
                    count ++;
                }
            }
            System.out.println(count + " failed");
        }
    }

    public SearchResponse simpleQuerySearch(String q) {
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery(q);
        return client.prepareSearch().setQuery(qb).get();
    }

    public void close() {
        client.close();
    }

    public static int getDefaultLoadSize() {
        return DEFAULT_LOAD_SIZE;
    }

    public static String getDefaultHost() {
        return DEFAULT_HOST;
    }

    public static int getDefaultPort() {
        return DEFAULT_PORT;
    }

}
