package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.RetainedDelivery;
import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.WalMessage;
import dev.j3rrryy.loommq.broker.topic.TopicFilterValidator;
import dev.j3rrryy.loommq.broker.topic.TopicMatcher;
import dev.j3rrryy.loommq.server.NetworkUtils;
import dev.j3rrryy.loommq.wal.Wal;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public final class SubscribeHandler extends SimpleChannelInboundHandler<MqttSubscribeMessage> {

    private final SubscriptionRegistry registry;
    private final Wal wal;
    private final AtomicInteger packetIdGenerator = new AtomicInteger(1);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        int packetId = msg.variableHeader().messageId();
        List<Integer> reasonCodes = new ArrayList<>();
        List<String> grantedFilters = new ArrayList<>();
        long replayOffset = getReplayOffset(msg);

        for (MqttTopicSubscription sub : msg.payload().topicSubscriptions()) {
            String requested = sub.topicFilter();
            int qos = sub.qualityOfService().value();

            if (!TopicFilterValidator.isValid(requested)) {
                log.warn("Invalid filter '{}' from {}", requested, NetworkUtils.remoteAddress(ctx.channel()));
                reasonCodes.add((int) MqttReasonCodes.SubAck.TOPIC_FILTER_INVALID.byteValue());
                grantedFilters.add(null);
                continue;
            }

            String actual = TopicFilterValidator.actualFilter(requested);
            String shareGroup = TopicFilterValidator.shareGroup(requested);

            if (user == null || !user.canSubscribe(actual)) {
                log.warn("User '{}' not authorized to subscribe to '{}'",
                        user != null ? user.username() : "unknown", requested);
                reasonCodes.add((int) MqttReasonCodes.SubAck.NOT_AUTHORIZED.byteValue());
                grantedFilters.add(null);
                continue;
            }

            registry.subscribe(ctx.channel(), requested, qos, shareGroup);
            reasonCodes.add(qos);
            grantedFilters.add(shareGroup == null ? actual : null);
        }

        ctx.writeAndFlush(buildSubAck(packetId, reasonCodes));

        for (int i = 0; i < grantedFilters.size(); i++) {
            if (grantedFilters.get(i) != null && reasonCodes.get(i) < 0x80) {
                if (replayOffset >= 0) {
                    startReplay(ctx, grantedFilters.get(i), replayOffset);
                } else {
                    sendRetainedMessages(ctx, grantedFilters.get(i));
                }
            }
        }
    }

    private long getReplayOffset(MqttSubscribeMessage msg) {
        MqttProperties props = msg.idAndPropertiesVariableHeader().properties();
        MqttProperties.UserProperties userProps = (MqttProperties.UserProperties) props.getProperty(MqttProperties.USER_PROPERTY);
        if (userProps != null) {
            for (MqttProperties.StringPair pair : userProps.value()) {
                if ("replay-from-offset".equals(pair.key)) {
                    try {
                        return Long.parseLong(pair.value);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return -1;
    }

    @SuppressWarnings("BusyWait")
    private void startReplay(ChannelHandlerContext ctx, String filter, long startOffset) {
        Thread.ofVirtual().start(() -> {
            log.info("Starting replay for filter '{}' from offset {}", filter, startOffset);
            long currentOffset = startOffset;
            long endOffset = wal.nextOffset();

            while (currentOffset < endOffset && ctx.channel().isActive()) {
                byte[] data = wal.read(currentOffset);
                if (data != null) {
                    WalMessage wm = WalMessage.fromBytes(data);
                    if (TopicMatcher.matches(filter, wm.topic())) {
                        while (!ctx.channel().isWritable()) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }

                        MqttMessage pub = MqttMessageBuilders.publish()
                                .topicName(wm.topic())
                                .qos(MqttQoS.AT_MOST_ONCE)
                                .payload(io.netty.buffer.Unpooled.wrappedBuffer(wm.payload()))
                                .build();
                        ctx.writeAndFlush(pub);
                    }
                }
                currentOffset++;
            }
            log.info("Replay finished for filter '{}'", filter);
        });
    }

    private void sendRetainedMessages(ChannelHandlerContext ctx, String filter) {
        List<RetainedDelivery> retained = registry.findRetained(filter);
        for (RetainedDelivery delivery : retained) {
            int qos = delivery.message().qos();
            MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                    .topicName(delivery.topic())
                    .retained(true)
                    .qos(MqttQoS.valueOf(qos))
                    .payload(delivery.message().payload().retainedDuplicate());

            if (qos > 0) {
                builder.messageId(nextPacketId());
            }

            ctx.write(builder.build());
            delivery.message().release();
        }
        if (!retained.isEmpty()) {
            ctx.flush();
        }
    }

    private int nextPacketId() {
        int id;
        do {
            id = Math.floorMod(packetIdGenerator.incrementAndGet(), 65535);
        } while (id == 0);
        return id;
    }

    private MqttSubAckMessage buildSubAck(int packetId, List<Integer> reasonCodes) {
        int[] codes = reasonCodes.stream().mapToInt(Integer::intValue).toArray();
        return new MqttSubAckMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttMessageIdAndPropertiesVariableHeader(packetId, MqttProperties.NO_PROPERTIES),
                new MqttSubAckPayload(codes)
        );
    }

}
