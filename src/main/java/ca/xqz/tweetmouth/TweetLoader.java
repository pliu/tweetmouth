package ca.xqz.tweetmouth;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.apache.commons.cli.*;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TweetLoader {

    private final static String DEFAULT_HOST = "127.0.0.1";
    private final static int DEFAULT_PORT = 9300;
    private final static int DEFAULT_LOAD_SIZE = 1000;

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option hostOption = new Option("h", "host", true, "The hostname of the ES node");
        options.addOption(hostOption);

        Option portOption = new Option("p", "port", true, "The port of the ES node");
        portOption.setType(Number.class);
        options.addOption(portOption);

        Option sizeOption = new Option("s", "size", true, "The number of tweets in a bundle");
        sizeOption.setType(Number.class);
        options.addOption(sizeOption);

        CommandLineParser clParser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = clParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("TweetLoader", options);
            System.exit(1);
        }

        String host = cmd.getOptionValue("host", DEFAULT_HOST);
        int port = DEFAULT_PORT, loadSize = DEFAULT_LOAD_SIZE;
        if (cmd.hasOption("port")) {
            port = ((Number) cmd.getParsedOptionValue("port")).intValue();
        }
        if (cmd.hasOption("size")) {
            loadSize = ((Number) cmd.getParsedOptionValue("size")).intValue();
        }

        /*
        String path = args[0];
        TweetParser parser = new TweetParser(path);

        Client client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        System.out.println("Successfully created a client");

        List<Map<String, String>> tweetMaps = parser.getMappedTweetsFromFile(loadSize);
        // while (tweetMaps.size() > 0) {
            for (Map<String, String> map : tweetMaps) {
                IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                        .source(jsonBuilder()
                                .startObject()
                                .field("name", "Joe Smith")
                                .field("gender", "male")
                                .endObject());
                UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
                        .doc(jsonBuilder()
                                .startObject()
                                .field("gender", "male")
                                .endObject())
                        .upsert(indexRequest);
                client.update(updateRequest).get();
            }
            tweetMaps = parser.getMappedTweetsFromFile(loadSize);
        // }

        client.close();*/
    }
}
