package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.Delivery;
import dev.j3rrryy.loommq.broker.RetainedMessage;
import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.topic.TopicValidator;
import dev.j3rrryy.loommq.server.NetworkUtils;
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
    private final Qos2Handler qos2Handler;
    private final AtomicInteger outboundPacketIdGenerator = new AtomicInteger(1);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        String topic = msg.variableHeader().topicName();
        int qos = msg.fixedHeader().qosLevel().value();
        boolean retain = msg.fixedHeader().isRetain();
        int packetId = msg.variableHeader().packetId();

        if (qos == 2) {
            if (qos2Handler.isIncomingDuplicate(packetId)) {
                sendPubRec(ctx, packetId);
                return;
            }
            qos2Handler.registerIncoming(packetId);
            sendPubRec(ctx, packetId);
        }

        if (!TopicValidator.isValid(topic)) {
            log.warn("Invalid PUBLISH topic '{}' from {}", topic, NetworkUtils.remoteAddress(ctx.channel()));
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
            int targetQos = target.qos();
            int outId = targetQos > 0 ? nextPacketId() : -1;

            if (targetQos == 2) {
                qos2Handler.trackOutbound(outId);
            }

            sendToSubscriber(target, topic, msg.payload(), retain, outId);
        }

        if (qos == 1) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                    .packetId(packetId)
                    .reasonCode(MqttReasonCodes.PubAck.SUCCESS.byteValue())
                    .build();
            ctx.writeAndFlush(pubAck);
        }
    }

    private void sendPubRec(ChannelHandlerContext ctx, int packetId) {
        MqttMessage pubRec = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(packetId),
                null
        );
        ctx.writeAndFlush(pubRec);
    }

    private void sendToSubscriber(Delivery target, String topic, ByteBuf payload, boolean retain, int packetId) {
        int targetQos = target.qos();
        MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                .topicName(topic)
                .qos(MqttQoS.valueOf(targetQos))
                .retained(retain)
                .payload(payload.retainedDuplicate());

        if (targetQos > 0 && packetId != -1) {
            builder.messageId(packetId);
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
