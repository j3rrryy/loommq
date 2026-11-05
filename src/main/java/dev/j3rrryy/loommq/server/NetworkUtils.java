package dev.j3rrryy.loommq.server;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public final class NetworkUtils {

    public static String remoteAddress(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        if (address instanceof InetSocketAddress inet) {
            String ip = inet.getAddress().getHostAddress();
            return ip + ":" + inet.getPort();
        }
        return String.valueOf(address);
    }

}
