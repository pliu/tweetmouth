package ca.xqz.tweetmouth;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

import static java.lang.System.exit;

public class TweetLoader {

    private final static String HOST = "127.0.0.1";
    private final static int CLIENT_PORT = 9300;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Takes 1 argument, the path to the file with the tweet data to be parsed and loaded");
            exit(1);
        }

        String path = args[0];
        TweetParser parser = new TweetParser(path);

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), CLIENT_PORT));
        System.out.println("Successfully created a client");

        client.close();
    }
}
