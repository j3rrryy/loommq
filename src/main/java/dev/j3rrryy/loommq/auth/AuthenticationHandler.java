package dev.j3rrryy.loommq.auth;

import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.Session;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.broker.session.WillMessage;
import dev.j3rrryy.loommq.broker.topic.TopicFilterValidator;
import dev.j3rrryy.loommq.server.NetworkUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
public final class AuthenticationHandler extends SimpleChannelInboundHandler<MqttMessage> {

    public static final AttributeKey<User> USER_KEY = AttributeKey.valueOf("loommq.user");
    public static final AttributeKey<Session> SESSION_KEY = AttributeKey.valueOf("loommq.session");

    private final UserStore userStore;
    private final ConnectionRateLimiter rateLimiter;
    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private final MeterRegistry meterRegistry;
    private final Counter rateLimitedCounter;

    public AuthenticationHandler(UserStore userStore, ConnectionRateLimiter rateLimiter, SessionRegistry sessionRegistry,
                                 SubscriptionRegistry subscriptionRegistry, MeterRegistry meterRegistry) {
        this.userStore = userStore;
        this.rateLimiter = rateLimiter;
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
        this.meterRegistry = meterRegistry;
        this.rateLimitedCounter = Counter.builder("loommq.auth.rate_limited").register(meterRegistry);
    }

    private static String getIp(ChannelHandlerContext ctx) {
        if (ctx.channel().remoteAddress() instanceof java.net.InetSocketAddress inet) {
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
            rateLimitedCounter.increment();
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY);
            return;
        }

        MqttConnectMessage connect = (MqttConnectMessage) msg;
        if (connect.variableHeader().version() != MqttVersion.MQTT_5.protocolLevel()) {
            reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return;
        }

        String clientId = connect.payload().clientIdentifier();
        boolean generatedClientId = false;

        if (clientId == null || clientId.isBlank()) {
            if (!connect.variableHeader().isCleanSession()) {
                reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID);
                return;
            }
            clientId = "auto-" + java.util.UUID.randomUUID();
            generatedClientId = true;
        }

        String username = connect.payload().userName();
        byte[] passwordBytes = connect.payload().passwordInBytes();
        String password = passwordBytes == null ? null : new String(passwordBytes, StandardCharsets.UTF_8);
        final String finalClientId = clientId;
        final boolean isGenerated = generatedClientId;

        userStore.authenticate(username, password).ifPresentOrElse(
                user -> {
                    rateLimiter.recordSuccess(ip);
                    onAuthenticated(ctx, user, connect, finalClientId, isGenerated);
                },
                () -> {
                    rateLimiter.recordFailure(ip);
                    reject(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                }
        );
    }

    private void onAuthenticated(ChannelHandlerContext ctx, User user, MqttConnectMessage connect, String clientId, boolean generatedClientId) {
        ctx.channel().attr(USER_KEY).set(user);
        boolean cleanStart = connect.variableHeader().isCleanSession();
        int sessionExpiry = getSessionExpiry(connect);
        int receiveMaximum = getReceiveMaximum(connect);

        Session session = cleanStart ? null : sessionRegistry.get(clientId);
        boolean sessionPresent = session != null;

        if (sessionPresent) {
            session.resetForReconnect(receiveMaximum);
            for (var entry : session.subscriptions().entrySet()) {
                subscriptionRegistry.subscribe(ctx.channel(), entry.getKey(), entry.getValue(),
                        TopicFilterValidator.shareGroup(entry.getKey()));
            }
        } else {
            if (cleanStart) {
                sessionRegistry.remove(clientId);
            }
            session = new Session(clientId, sessionExpiry, receiveMaximum, meterRegistry);
            sessionRegistry.put(session);
        }

        ctx.channel().attr(SESSION_KEY).set(session);
        if (connect.variableHeader().isWillFlag()) {
            WillMessage will = new WillMessage(
                    connect.payload().willTopic(),
                    connect.payload().willMessageInBytes(),
                    connect.variableHeader().willQos(),
                    connect.variableHeader().isWillRetain()
            );
            ctx.channel().attr(WillMessage.WILL_KEY).set(will);
        }
        log.info("Authenticated user={} clientId={} from {}", user.username(), clientId, NetworkUtils.remoteAddress(ctx.channel()));

        MqttProperties connAckProps = MqttProperties.NO_PROPERTIES;
        if (generatedClientId) {
            connAckProps = new MqttProperties();
            connAckProps.add(new MqttProperties.StringProperty(MqttProperties.ASSIGNED_CLIENT_IDENTIFIER, clientId));
        }
        MqttMessage connAck = MqttMessageBuilders.connAck()
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .sessionPresent(sessionPresent)
                .properties(connAckProps)
                .build();
        ctx.writeAndFlush(connAck);
        ctx.pipeline().remove(this);
    }

    private int getSessionExpiry(MqttConnectMessage connect) {
        MqttProperties properties = connect.variableHeader().properties();
        if (properties != null) {
            MqttProperties.IntegerProperty prop = (MqttProperties.IntegerProperty) properties.getProperty(MqttProperties.SESSION_EXPIRY_INTERVAL);
            if (prop != null) {
                return prop.value();
            }
        }
        return 0;
    }

    private int getReceiveMaximum(MqttConnectMessage connect) {
        MqttProperties properties = connect.variableHeader().properties();
        if (properties != null) {
            MqttProperties.IntegerProperty prop = (MqttProperties.IntegerProperty) properties.getProperty(MqttProperties.RECEIVE_MAXIMUM);
            if (prop != null) {
                return prop.value();
            }
        }
        return 65535;
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
