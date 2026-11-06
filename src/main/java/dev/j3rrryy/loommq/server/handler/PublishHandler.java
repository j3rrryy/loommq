package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.retained.RetainedMessage;
import dev.j3rrryy.loommq.broker.routing.Delivery;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.Session;
import dev.j3rrryy.loommq.broker.topic.TopicValidator;
import dev.j3rrryy.loommq.broker.wal.WalMessage;
import dev.j3rrryy.loommq.server.NetworkUtils;
import dev.j3rrryy.loommq.wal.Wal;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class PublishHandler extends SimpleChannelInboundHandler<MqttPublishMessage> {

    private final SubscriptionRegistry registry;
    private final Qos2Handler qos2Handler;
    private final Wal wal;
    private final AtomicInteger outboundPacketIdGenerator = new AtomicInteger(1);
    private final Counter publishCounter;
    private final Counter deliverCounter;
    private final Counter bytesReceivedCounter;
    private final Counter bytesSentCounter;
    private final Counter droppedInvalidTopicCounter;
    private final Counter droppedUnauthorizedCounter;
    private final Counter droppedBackpressureCounter;
    private final Counter backpressureEventsCounter;
    private final Timer walAppendTimer;
    private final Timer deliveryTimer;

    public PublishHandler(SubscriptionRegistry registry, Qos2Handler qos2Handler, Wal wal, MeterRegistry meterRegistry) {
        this.registry = registry;
        this.qos2Handler = qos2Handler;
        this.wal = wal;
        this.publishCounter = Counter.builder("loommq.messages.publish").register(meterRegistry);
        this.deliverCounter = Counter.builder("loommq.messages.deliver").register(meterRegistry);
        this.bytesReceivedCounter = Counter.builder("loommq.bytes.received").register(meterRegistry);
        this.bytesSentCounter = Counter.builder("loommq.bytes.sent").register(meterRegistry);
        this.droppedInvalidTopicCounter = Counter.builder("loommq.messages.dropped")
                .tag("reason", "invalid_topic")
                .register(meterRegistry);
        this.droppedUnauthorizedCounter = Counter.builder("loommq.messages.dropped")
                .tag("reason", "unauthorized")
                .register(meterRegistry);
        this.droppedBackpressureCounter = Counter.builder("loommq.messages.dropped")
                .tag("reason", "backpressure")
                .register(meterRegistry);
        this.backpressureEventsCounter = Counter.builder("loommq.backpressure.events").register(meterRegistry);
        this.walAppendTimer = Timer.builder("loommq.latency.wal_append").register(meterRegistry);
        this.deliveryTimer = Timer.builder("loommq.latency.delivery").register(meterRegistry);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        String topic = msg.variableHeader().topicName();
        int qos = msg.fixedHeader().qosLevel().value();
        boolean retain = msg.fixedHeader().isRetain();
        int packetId = msg.variableHeader().packetId();

        if (!TopicValidator.isValid(topic)) {
            droppedInvalidTopicCounter.increment();
            log.warn("Invalid PUBLISH topic '{}' from {}", topic, NetworkUtils.remoteAddress(ctx.channel()));
            sendError(ctx, qos, packetId, MqttReasonCodes.PubAck.TOPIC_NAME_INVALID.byteValue());
            return;
        }

        if (user == null || !user.canPublish(topic)) {
            droppedUnauthorizedCounter.increment();
            log.warn("User '{}' not authorized to publish to '{}'",
                    user != null ? user.username() : "unknown", topic);
            sendError(ctx, qos, packetId, MqttReasonCodes.PubAck.NOT_AUTHORIZED.byteValue());
            return;
        }

        if (qos == 2) {
            if (qos2Handler.isIncomingDuplicate(packetId)) {
                sendPubRec(ctx, packetId);
                return;
            }
            qos2Handler.registerIncoming(packetId);
            sendPubRec(ctx, packetId);
        }

        byte[] payloadBytes = new byte[msg.payload().readableBytes()];
        msg.payload().getBytes(msg.payload().readerIndex(), payloadBytes);

        WalMessage walMessage = new WalMessage(topic, payloadBytes);
        walAppendTimer.record(() -> wal.append(walMessage.toBytes()));
        publishCounter.increment();
        bytesReceivedCounter.increment(payloadBytes.length);

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
        MqttPublishMessage pub = builder.build();

        if (targetQos > 0) {
            Session session = target.channel().attr(AuthenticationHandler.SESSION_KEY).get();
            if (session != null) {
                if (session.inFlightCount().get() >= session.receiveMaximum()) {
                    if (session.pendingMessages().size() >= Session.MAX_PENDING_MESSAGES) {
                        pub.release();
                        droppedBackpressureCounter.increment();
                        return;
                    }
                    boolean isQueueEmpty = session.pendingMessages().isEmpty();
                    session.pendingMessages().add(pub);
                    if (isQueueEmpty) {
                        backpressureEventsCounter.increment();
                        log.warn("Backpressure started for client {}. In-flight: {}, Max: {}",
                                NetworkUtils.remoteAddress(target.channel()),
                                session.inFlightCount().get(),
                                session.receiveMaximum());
                    }
                    return;
                }
                session.inFlightCount().incrementAndGet();
            }
        }
        deliveryTimer.record(() -> {
            target.channel().writeAndFlush(pub);
        });
        deliverCounter.increment();
        bytesSentCounter.increment(payload.readableBytes());
    }

    private void sendError(ChannelHandlerContext ctx, int qos, int packetId, byte reasonCode) {
        if (qos == 1) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                    .packetId(packetId)
                    .reasonCode(reasonCode)
                    .build();
            ctx.writeAndFlush(pubAck);
        } else if (qos == 2) {
            MqttMessage pubRec = new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttPubReplyMessageVariableHeader(packetId, reasonCode, MqttProperties.NO_PROPERTIES)
            );
            ctx.writeAndFlush(pubRec);
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
