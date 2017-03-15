package ca.xqz.tweetmouth;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.transport.client.PreBuiltTransportClient;

class ESUtil {
    private final static int CLIENT_PORT = 9300;

    public static TransportClient getTransportClient() {
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

}
