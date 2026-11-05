package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import dev.j3rrryy.loommq.server.NetworkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public final class MqttConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final MqttMessage PINGRESP = new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));

    private final SubscriptionRegistry registry;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("Client connected from {}", NetworkUtils.remoteAddress(ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug("Client disconnected from {}", NetworkUtils.remoteAddress(ctx.channel()));
        registry.removeChannel(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.fixedHeader() == null) {
            return;
        }
        switch (msg.fixedHeader().messageType()) {
            case PINGREQ -> ctx.writeAndFlush(PINGRESP);
            case DISCONNECT -> ctx.close();
            case PUBLISH, SUBSCRIBE, UNSUBSCRIBE -> {
            }
            default -> log.debug("Unhandled message type from {}: {}",
                    NetworkUtils.remoteAddress(ctx.channel()), msg.fixedHeader().messageType());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.debug("Client idle timeout: {}", NetworkUtils.remoteAddress(ctx.channel()));
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception on channel {}", NetworkUtils.remoteAddress(ctx.channel()), cause);
        ctx.close();
    }

}
