package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.broker.routing.Delivery;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.Session;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.broker.session.WillMessage;
import dev.j3rrryy.loommq.broker.wal.WalMessage;
import dev.j3rrryy.loommq.server.NetworkUtils;
import dev.j3rrryy.loommq.wal.Wal;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public final class MqttConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final MqttMessage PINGRESP = new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));

    private final SubscriptionRegistry registry;
    private final Wal wal;
    private final SessionRegistry sessionRegistry;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("Client connected from {}", NetworkUtils.remoteAddress(ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug("Client disconnected from {}", NetworkUtils.remoteAddress(ctx.channel()));
        WillMessage will = ctx.channel().attr(WillMessage.WILL_KEY).get();
        if (will != null) {
            publishWill(will);
        }

        Session session = ctx.channel().attr(AuthenticationHandler.SESSION_KEY).get();
        if (session != null && session.sessionExpiryInterval() == 0) {
            sessionRegistry.remove(session.clientId());
        }
        registry.removeChannel(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.fixedHeader() == null) {
            return;
        }
        switch (msg.fixedHeader().messageType()) {
            case PINGREQ -> ctx.writeAndFlush(PINGRESP);
            case DISCONNECT -> {
                ctx.channel().attr(WillMessage.WILL_KEY).set(null);
                ctx.close();
            }
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

    private void publishWill(WillMessage will) {
        log.info("Publishing Will message for topic '{}'", will.topic());
        WalMessage walMsg = new WalMessage(will.topic(), will.payload());
        wal.append(walMsg.toBytes());

        List<Delivery> targets = registry.findTargets(will.topic(), will.qos());
        for (Delivery target : targets) {
            MqttMessage pub = MqttMessageBuilders.publish()
                    .topicName(will.topic())
                    .qos(MqttQoS.AT_MOST_ONCE)
                    .retained(will.retain())
                    .payload(Unpooled.wrappedBuffer(will.payload()))
                    .build();
            target.channel().writeAndFlush(pub);
        }
    }

}
