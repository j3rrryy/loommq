package dev.j3rrryy.loommq.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MqttConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final MqttMessage PINGRESP = new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("Client connected from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug("Client disconnected from {}", ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case PINGREQ -> ctx.writeAndFlush(PINGRESP);
            case DISCONNECT -> ctx.close();
            default -> log.debug("Unhandled message type from {}: {}",
                    ctx.channel().remoteAddress(), msg.fixedHeader().messageType());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.debug("Client idle timeout: {}", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception on channel {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }

}
