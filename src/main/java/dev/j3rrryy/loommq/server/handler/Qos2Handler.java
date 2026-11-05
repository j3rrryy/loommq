package dev.j3rrryy.loommq.server.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class Qos2Handler extends ChannelInboundHandlerAdapter {

    private final Set<Integer> incoming = ConcurrentHashMap.newKeySet();
    private final Set<Integer> awaitingPubRec = ConcurrentHashMap.newKeySet();
    private final Set<Integer> awaitingPubComp = ConcurrentHashMap.newKeySet();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof MqttMessage mqttMsg) || mqttMsg.fixedHeader() == null) {
            ctx.fireChannelRead(msg);
            return;
        }

        switch (mqttMsg.fixedHeader().messageType()) {
            case PUBREL -> {
                onPubRel(ctx, mqttMsg);
                ReferenceCountUtil.release(msg);
            }
            case PUBREC -> {
                onPubRec(ctx, mqttMsg);
                ReferenceCountUtil.release(msg);
            }
            case PUBCOMP -> {
                onPubComp(mqttMsg);
                ReferenceCountUtil.release(msg);
            }
            default -> ctx.fireChannelRead(msg);
        }
    }

    private void onPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
        int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        incoming.remove(packetId);

        MqttMessage pubComp = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(packetId),
                null
        );
        ctx.writeAndFlush(pubComp);
    }

    private void onPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
        int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        awaitingPubRec.remove(packetId);
        awaitingPubComp.add(packetId);

        MqttMessage pubRel = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                MqttMessageIdVariableHeader.from(packetId),
                null
        );
        ctx.writeAndFlush(pubRel);
    }

    private void onPubComp(MqttMessage msg) {
        int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        awaitingPubComp.remove(packetId);
    }

    public boolean isIncomingDuplicate(int packetId) {
        return incoming.contains(packetId);
    }

    public void registerIncoming(int packetId) {
        incoming.add(packetId);
    }

    public void trackOutbound(int packetId) {
        awaitingPubRec.add(packetId);
    }

}
