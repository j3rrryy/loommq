package dev.j3rrryy.loommq.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.j3rrryy.loommq.auth.AuthenticationHandler;
import dev.j3rrryy.loommq.auth.ConnectionRateLimiter;
import dev.j3rrryy.loommq.auth.UserStore;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.server.handler.MqttConnectionHandler;
import dev.j3rrryy.loommq.server.handler.PublishHandler;
import dev.j3rrryy.loommq.server.handler.Qos2Handler;
import dev.j3rrryy.loommq.server.handler.SubscribeHandler;
import dev.j3rrryy.loommq.server.handler.UnsubscribeHandler;
import dev.j3rrryy.loommq.wal.Wal;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final int IDLE_TIMEOUT_SECONDS = 120;
    private static final int MAX_BYTES_IN_MESSAGE = 16 * 1024 * 1024;
    private static final int MAX_CLIENT_ID_LENGTH = 65535;

    private final UserStore userStore;
    private final ConnectionRateLimiter rateLimiter;
    private final SubscriptionRegistry registry;
    private final SessionRegistry sessionRegistry;
    private final Wal wal;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeConnections;

    @Override
    protected void initChannel(SocketChannel ch) {
        Qos2Handler qos2Handler = new Qos2Handler();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("idleState", new IdleStateHandler(0, 0, IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        pipeline.addLast("mqttDecoder", new MqttDecoder(MAX_BYTES_IN_MESSAGE, MAX_CLIENT_ID_LENGTH));
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("authentication", new AuthenticationHandler(userStore, rateLimiter, sessionRegistry,
                registry, meterRegistry));
        pipeline.addLast("qos2Handler", qos2Handler);
        pipeline.addLast("publishHandler", new PublishHandler(registry, qos2Handler, wal, meterRegistry));
        pipeline.addLast("subscribeHandler", new SubscribeHandler(registry, wal));
        pipeline.addLast("unsubscribeHandler", new UnsubscribeHandler(registry));
        pipeline.addLast("mqttHandler", new MqttConnectionHandler(registry, wal, sessionRegistry, meterRegistry,
                activeConnections));
    }

}
