package dev.j3rrryy.loommq.auth;

import dev.j3rrryy.loommq.server.NetworkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.AttributeKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

@Slf4j
@RequiredArgsConstructor
public final class AuthenticationHandler extends SimpleChannelInboundHandler<MqttMessage> {

    public static final AttributeKey<User> USER_KEY = AttributeKey.valueOf("loommq.user");

    private final UserStore userStore;
    private final ConnectionRateLimiter rateLimiter;

    private static String getIp(ChannelHandlerContext ctx) {
        if (ctx.channel().remoteAddress() instanceof InetSocketAddress inet) {
            return inet.getAddress().getHostAddress();
        }
        return "unknown";
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.fixedHeader().messageType() != MqttMessageType.CONNECT) {
            ctx.fireChannelRead(msg);
            return;
        }

        String ip = getIp(ctx);
        if (!rateLimiter.isAllowed(ip)) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY);
            return;
        }

        MqttConnectMessage connect = (MqttConnectMessage) msg;
        if (connect.variableHeader().version() != MqttVersion.MQTT_5.protocolLevel()) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return;
        }

        String username = connect.payload().userName();
        byte[] passwordBytes = connect.payload().passwordInBytes();
        String password = passwordBytes == null ? null : new String(passwordBytes, StandardCharsets.UTF_8);

        userStore.authenticate(username, password).ifPresentOrElse(
                user -> {
                    rateLimiter.recordSuccess(ip);
                    onAuthenticated(ctx, user);
                },
                () -> {
                    rateLimiter.recordFailure(ip);
                    reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                }
        );
    }

    private void onAuthenticated(ChannelHandlerContext ctx, User user) {
        ctx.channel().attr(USER_KEY).set(user);
        log.info("Authenticated user={} from {}", user.username(), NetworkUtils.remoteAddress(ctx.channel()));

        MqttMessage connAck = MqttMessageBuilders.connAck()
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .sessionPresent(false)
                .build();
        ctx.writeAndFlush(connAck);

        ctx.pipeline().remove(this);
    }

    private void reject(ChannelHandlerContext ctx, MqttConnectReturnCode code) {
        log.warn("Rejected CONNECT from {}: {}", NetworkUtils.remoteAddress(ctx.channel()), code);
        MqttMessage connAck = MqttMessageBuilders.connAck()
                .returnCode(code)
                .sessionPresent(false)
                .build();
        ctx.writeAndFlush(connAck).addListener(_ -> ctx.close());
    }

}
