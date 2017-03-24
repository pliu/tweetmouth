package ca.xqz.tweetmouth;

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

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.List;

class ESClient {

    private final static String DEFAULT_HOST = "127.0.0.1";
    private final static int DEFAULT_PORT = 9300;
    private final static int DEFAULT_LOAD_SIZE = 1000;

    private TransportClient client;
    private String index;
    private String type;
    private final Function<String, String> MAPPING = type -> {
        return "{\n" +
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
        "  }";
    };

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
        IndicesExistsResponse resp = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (resp.isExists())
            return;

        System.out.println("Index doesn't exist; creating it");
        client.admin().indices().prepareCreate(index)
            .addMapping(type, MAPPING.apply(type))
            .get();
    }

    // TODO: Add buffering if we hook this up to the Twitter stream
    public void loadTweets(Stream<String> tweets) {
        final BulkRequestBuilder bulkRequest = client.prepareBulk();
        tweets.forEach( tweet -> {
                bulkRequest.add(new IndexRequest(index, type, tweet));
            });

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
