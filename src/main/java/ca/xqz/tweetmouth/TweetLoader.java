package ca.xqz.tweetmouth;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.io.IOException;

import java.util.List;

public class TweetLoader {

    public static void main(String[] args) {
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

        String host = cmd.getOptionValue("host", ESClient.getDefaultHost());
        int port = ESClient.getDefaultPort();
        int loadSize = ESClient.getDefaultLoadSize();

        try {
            if (cmd.hasOption("port")) {
                port = ((Number) cmd.getParsedOptionValue("port")).intValue();
            }
            if (cmd.hasOption("size")) {
                loadSize = ((Number) cmd.getParsedOptionValue("size")).intValue();
            }
        } catch (ParseException e) {
            System.err.println("Error parsing options: " + e);
            return;
        }

        TweetParser parser = new TweetParser();
        ESClient client = new ESClient(host, port, "test_index", "tweet");

        System.out.println("Loading data");
        List<Tweet> tweets;
        int iter = 1;
        do {
            try {
                tweets = parser.getParsedTweets(loadSize);
            } catch (IOException e) {
                System.err.println("Failed to process tweets.");
                return;
            }
            if (tweets.size() <= 0)
                break;
            client.loadTweets(tweets);
            System.out.println("Processed " + loadSize*(iter ++));
         } while (true);
    }
}
