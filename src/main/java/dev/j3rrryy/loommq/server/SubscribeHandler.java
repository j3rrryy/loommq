package dev.j3rrryy.loommq.server;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.User;
import dev.j3rrryy.loommq.broker.RetainedDelivery;
import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.TopicFilterValidator;
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
    private final AtomicInteger packetIdGenerator = new AtomicInteger(1);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        User user = ctx.channel().attr(AuthenticationHandler.USER_KEY).get();
        int packetId = msg.variableHeader().messageId();
        List<Integer> reasonCodes = new ArrayList<>();
        List<String> grantedFilters = new ArrayList<>();

        for (MqttTopicSubscription sub : msg.payload().topicSubscriptions()) {
            String requested = sub.topicFilter();
            int qos = sub.qualityOfService().value();

            if (!TopicFilterValidator.isValid(requested)) {
                log.warn("Invalid filter '{}' from {}", requested, ctx.channel().remoteAddress());
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
            grantedFilters.add(actual);
        }

        ctx.writeAndFlush(buildSubAck(packetId, reasonCodes));

        for (int i = 0; i < grantedFilters.size(); i++) {
            if (grantedFilters.get(i) != null && reasonCodes.get(i) < 0x80) {
                sendRetainedMessages(ctx, grantedFilters.get(i));
            }
        }
    }

    private void sendRetainedMessages(ChannelHandlerContext ctx, String filter) {
        List<RetainedDelivery> retained = registry.findRetained(filter);
        for (RetainedDelivery delivery : retained) {
            int qos = delivery.message().qos();
            MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                    .topicName(delivery.topic())
                    .retained(true)
                    .qos(MqttQoS.valueOf(qos))
                    .payload(delivery.message().payload());

            if (qos > 0) {
                builder.messageId(nextPacketId());
            }

            MqttPublishMessage pub = builder.build();
            ctx.write(pub);
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
