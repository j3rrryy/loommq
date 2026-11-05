package dev.j3rrryy.loommq.server;

import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

@Slf4j
@RequiredArgsConstructor
public final class MqttConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final MqttMessage PINGRESP = new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));

    private final SubscriptionRegistry registry;

    private static String formatAddress(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        if (address instanceof InetSocketAddress inet) {
            return inet.getAddress().getHostAddress() + ":" + inet.getPort();
        }
        return String.valueOf(address);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("Client connected from {}", formatAddress(ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug("Client disconnected from {}", formatAddress(ctx.channel()));
        registry.removeChannel(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case PINGREQ -> ctx.writeAndFlush(PINGRESP);
            case DISCONNECT -> ctx.close();
            case PUBLISH, SUBSCRIBE, UNSUBSCRIBE -> {
            }
            default -> log.debug("Unhandled message type from {}: {}",
                    formatAddress(ctx.channel()), msg.fixedHeader().messageType());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.debug("Client idle timeout: {}", formatAddress(ctx.channel()));
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception on channel {}", formatAddress(ctx.channel()), cause);
        ctx.close();
    }

}
