package dev.j3rrryy.loommq.server;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public final class PublishHandler extends SimpleChannelInboundHandler<MqttPublishMessage> {

    private final SubscriptionRegistry registry;
    private final AtomicInteger outboundPacketIdGenerator = new AtomicInteger(1);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        String topic = msg.variableHeader().topicName();
        int qos = msg.fixedHeader().qosLevel().value();
        boolean retain = msg.fixedHeader().isRetain();
        int packetId = msg.variableHeader().packetId();

        if (!TopicValidator.isValid(topic)) {
            log.warn("Invalid PUBLISH topic '{}' from {}", topic, ctx.channel().remoteAddress());
            sendError(ctx, qos, packetId, MqttReasonCodes.PubAck.TOPIC_NAME_INVALID.byteValue());
            return;
        }

        if (user == null || !user.canPublish(topic)) {
            log.warn("User '{}' not authorized to publish to '{}'",
                    user != null ? user.username() : "unknown", topic);
            sendError(ctx, qos, packetId, MqttReasonCodes.PubAck.NOT_AUTHORIZED.byteValue());
            return;
        }

        if (retain) {
            if (msg.payload().readableBytes() == 0) {
                registry.clearRetained(topic);
            } else {
                long expiresAt = getExpiryInterval(msg);
                RetainedMessage retainedMsg = new RetainedMessage(msg.payload().retainedDuplicate(), qos, expiresAt);
                registry.storeRetained(topic, retainedMsg);
            }
        }

        List<Delivery> targets = registry.findTargets(topic, qos);
        for (Delivery target : targets) {
            sendToSubscriber(target, topic, msg.payload(), retain);
        }

        if (qos == 1) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                    .packetId(packetId)
                    .reasonCode(MqttReasonCodes.PubAck.SUCCESS.byteValue())
                    .build();
            ctx.writeAndFlush(pubAck);
        }
    }

    private void sendToSubscriber(Delivery target, String topic, ByteBuf payload, boolean retain) {
        int targetQos = target.qos();
        MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                .topicName(topic)
                .qos(MqttQoS.valueOf(targetQos))
                .retained(retain)
                .payload(payload.retainedDuplicate());

        if (targetQos > 0) {
            builder.messageId(nextPacketId());
        }

        target.channel().writeAndFlush(builder.build());
    }

    private void sendError(ChannelHandlerContext ctx, int qos, int packetId, byte reasonCode) {
        if (qos == 1) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                    .packetId(packetId)
                    .reasonCode(reasonCode)
                    .build();
            ctx.writeAndFlush(pubAck);
        }
    }

    private long getExpiryInterval(MqttPublishMessage msg) {
        MqttProperties.IntegerProperty prop = (MqttProperties.IntegerProperty)
                msg.variableHeader().properties().getProperty(MqttProperties.PUBLICATION_EXPIRY_INTERVAL);
        if (prop != null) {
            return System.currentTimeMillis() + (prop.value() * 1000L);
        }
        return 0;
    }

    private int nextPacketId() {
        int id;
        do {
            id = Math.floorMod(outboundPacketIdGenerator.incrementAndGet(), 65535);
        } while (id == 0);
        return id;
    }

}
