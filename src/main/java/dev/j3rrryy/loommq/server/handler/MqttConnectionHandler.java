package dev.j3rrryy.loommq.server.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.retained.RetainedMessage;
import dev.j3rrryy.loommq.broker.routing.Delivery;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.Session;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.broker.session.WillMessage;
import dev.j3rrryy.loommq.broker.topic.TopicValidator;
import dev.j3rrryy.loommq.broker.wal.WalMessage;
import dev.j3rrryy.loommq.server.NetworkUtils;
import dev.j3rrryy.loommq.wal.Wal;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MqttConnectionHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final MqttMessage PINGRESP = new MqttMessage(
            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));

    private final SubscriptionRegistry registry;
    private final Wal wal;
    private final SessionRegistry sessionRegistry;
    private final AtomicInteger activeConnections;
    private final Counter connectionsTotal;
    private final AtomicInteger outboundPacketIdGenerator = new AtomicInteger(1);

    public MqttConnectionHandler(SubscriptionRegistry registry, Wal wal, SessionRegistry sessionRegistry,
                                 MeterRegistry meterRegistry, AtomicInteger activeConnections) {
        this.registry = registry;
        this.wal = wal;
        this.sessionRegistry = sessionRegistry;
        this.activeConnections = activeConnections;
        this.connectionsTotal = Counter.builder("loommq.connections.total").register(meterRegistry);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        activeConnections.incrementAndGet();
        connectionsTotal.increment();
        log.debug("Client connected from {}", NetworkUtils.remoteAddress(ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        activeConnections.decrementAndGet();
        log.debug("Client disconnected from {}", NetworkUtils.remoteAddress(ctx.channel()));
        WillMessage will = ctx.channel().attr(WillMessage.WILL_KEY).get();
        if (will != null) {
            publishWill(ctx, will);
        }

        Session session = ctx.channel().attr(AuthenticationHandler.SESSION_KEY).get();
        if (session != null && session.sessionExpiryInterval() == 0) {
            sessionRegistry.remove(session.clientId(), session);
        }
        registry.removeChannel(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.fixedHeader() == null) {
            return;
        }
        switch (msg.fixedHeader().messageType()) {
            case PINGREQ -> {
                var _ = ctx.writeAndFlush(PINGRESP);
            }
            case DISCONNECT -> {
                ctx.channel().attr(WillMessage.WILL_KEY).set(null);
                var _ = ctx.close();
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
            var _ = ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception on channel {}", NetworkUtils.remoteAddress(ctx.channel()), cause);
        var _ = ctx.close();
    }

    private void publishWill(ChannelHandlerContext ctx, WillMessage will) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        if (user == null || !TopicValidator.isValid(will.topic()) || !user.canPublish(will.topic())) {
            log.warn("Rejected Will message for topic '{}'", will.topic());
            return;
        }

        log.info("Publishing Will message for topic '{}'", will.topic());
        WalMessage walMsg = new WalMessage(will.topic(), will.payload());
        wal.append(walMsg.toBytes());

        if (will.retain()) {
            if (will.payload().readableBytes() == 0) {
                registry.clearRetained(will.topic());
            } else {
                registry.storeRetained(will.topic(),
                        new RetainedMessage(will.payload().retainedDuplicate(), will.qos(), 0));
            }
        }

        List<Delivery> targets = registry.findTargets(will.topic(), will.qos());
        for (Delivery target : targets) {
            int targetQos = target.qos();
            int packetId = targetQos > 0 ? nextPacketId() : -1;

            MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                    .topicName(will.topic())
                    .qos(MqttQoS.valueOf(targetQos))
                    .retained(false)
                    .payload(will.payload());

            if (targetQos > 0) {
                builder.messageId(packetId);
            }
            var _ = target.channel().writeAndFlush(builder.build());
        }
    }

    private int nextPacketId() {
        int id;
        do {
            id = Math.floorMod(outboundPacketIdGenerator.incrementAndGet(), 65535);
        } while (id == 0);
        return id;
    }

}
