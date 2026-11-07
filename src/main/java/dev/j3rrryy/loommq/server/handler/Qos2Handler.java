package dev.j3rrryy.loommq.server.handler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.broker.session.Session;
import dev.j3rrryy.loommq.server.NetworkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
            case PUBACK, PUBCOMP -> {
                handleAck(ctx);
                ReferenceCountUtil.release(msg);
            }
            case PUBREC -> {
                onPubRec(ctx, mqttMsg);
                ReferenceCountUtil.release(msg);
            }
            case PUBREL -> {
                onPubRel(ctx, mqttMsg);
                ReferenceCountUtil.release(msg);
            }
            default -> ctx.fireChannelRead(msg);
        }
    }

    private void handleAck(ChannelHandlerContext ctx) {
        Session session = ctx.channel().attr(AuthenticationHandler.SESSION_KEY).get();
        if (session != null) {
            session.inFlightCount().decrementAndGet();
            processPendingQueue(ctx, session);
        }
    }

    private void processPendingQueue(ChannelHandlerContext ctx, Session session) {
        boolean hadPending = !session.pendingMessages().isEmpty();
        while (session.inFlightCount().get() < session.receiveMaximum() && !session.pendingMessages().isEmpty()) {
            MqttPublishMessage pending = session.pendingMessages().poll();
            session.inFlightCount().incrementAndGet();
            ctx.writeAndFlush(pending);
        }
        if (hadPending && session.pendingMessages().isEmpty()) {
            log.info("Backpressure resolved for client {}", NetworkUtils.remoteAddress(ctx.channel()));
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
