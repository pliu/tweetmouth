package ca.xqz.tweetmouth;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.List;

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

        Option fileOption = new Option("f", "file", true, "The path to the  tweet file");
        fileOption.setRequired(true);
        options.addOption(fileOption);

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
        String path = cmd.getOptionValue("file");

        TweetParser parser = new TweetParser(new FileInputStream(path));
        Gson gson = new Gson();

        Client client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        System.out.println("Successfully created a client");

        List<Tweet> tweets = parser.getParsedTweets(loadSize);
        while (tweets.size() > 0) {
            for (Tweet tweet : tweets) {
                IndexRequest indexRequest = new IndexRequest("index", "type", Long.toString(tweet.getId()))
                        .source(gson.toJson(tweet));
                UpdateRequest updateRequest = new UpdateRequest("index", "type", Long.toString(tweet.getId()))
                        .doc(gson.toJson(tweet))
                        .upsert(indexRequest);
                client.update(updateRequest).get();
            }
            tweets = parser.getParsedTweets(loadSize);
        }
        client.close();
    }
}
