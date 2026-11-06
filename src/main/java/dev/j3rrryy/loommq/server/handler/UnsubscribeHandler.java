package dev.j3rrryy.loommq.server.handler;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.Session;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public final class UnsubscribeHandler extends SimpleChannelInboundHandler<MqttUnsubscribeMessage> {

    private final SubscriptionRegistry registry;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        int packetId = msg.variableHeader().messageId();
        List<Short> reasonCodes = new ArrayList<>();

        for (String topicFilter : msg.payload().topics()) {
            registry.unsubscribe(ctx.channel(), topicFilter);
            Session session = ctx.channel().attr(AuthenticationHandler.SESSION_KEY).get();
            if (session != null) {
                session.removeSubscription(topicFilter);
            }
            reasonCodes.add((short) MqttReasonCodes.UnsubAck.SUCCESS.byteValue());
        }

        short[] codes = new short[reasonCodes.size()];
        for (int i = 0; i < reasonCodes.size(); i++) {
            codes[i] = reasonCodes.get(i);
        }

        MqttUnsubAckMessage unsubAck = new MqttUnsubAckMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttMessageIdAndPropertiesVariableHeader(packetId, MqttProperties.NO_PROPERTIES),
                new MqttUnsubAckPayload(codes)
        );

        ctx.writeAndFlush(unsubAck);
    }

}
