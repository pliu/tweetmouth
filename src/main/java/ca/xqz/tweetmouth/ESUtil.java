package ca.xqz.tweetmouth;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.transport.client.PreBuiltTransportClient;

class ESUtil {
    private final static String DEFAULT_HOST = "127.0.0.1";
    private final static int DEFAULT_PORT = 9300;

    public static String getDefaultHost() {
        return DEFAULT_HOST;
    }

    public static int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public static TransportClient getTransportClient(String host, int port) {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return new PreBuiltTransportClient(Settings.EMPTY)
            .addTransportAddress(new InetSocketTransportAddress(inetAddr, port));
    }

    public static TransportClient getTransportClient() {
        return getTransportClient(DEFAULT_HOST, DEFAULT_PORT);
    }
}
