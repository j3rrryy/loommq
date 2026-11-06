package dev.j3rrryy.loommq.broker.session;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class Session {

    public static final int MAX_PENDING_MESSAGES = 10_000;
    private static final int DEFAULT_RECEIVE_MAXIMUM = 65535;

    private final String clientId;
    private final int sessionExpiryInterval;
    private final ConcurrentHashMap<String, Integer> subscriptions = new ConcurrentHashMap<>();
    private final AtomicInteger inFlightCount = new AtomicInteger(0);
    private final Queue<MqttPublishMessage> pendingMessages = new ConcurrentLinkedQueue<>();
    private final Gauge queueSizeGauge;
    private int receiveMaximum;

    public Session(String clientId, int sessionExpiryInterval, int receiveMaximum, MeterRegistry registry) {
        this.clientId = clientId;
        this.sessionExpiryInterval = sessionExpiryInterval;
        this.receiveMaximum = normalizeReceiveMaximum(receiveMaximum);
        this.queueSizeGauge = Gauge.builder("loommq.queue.size", pendingMessages, Queue::size)
                .tag("client", clientId)
                .register(registry);
    }

    private static int normalizeReceiveMaximum(int receiveMaximum) {
        return receiveMaximum == 0 ? DEFAULT_RECEIVE_MAXIMUM : receiveMaximum;
    }

    public String clientId() {
        return clientId;
    }

    public int sessionExpiryInterval() {
        return sessionExpiryInterval;
    }

    public ConcurrentHashMap<String, Integer> subscriptions() {
        return subscriptions;
    }

    public void addSubscription(String filter, int qos) {
        subscriptions.put(filter, qos);
    }

    public void removeSubscription(String filter) {
        subscriptions.remove(filter);
    }

    public int receiveMaximum() {
        return receiveMaximum;
    }

    public void resetForReconnect(int newReceiveMaximum) {
        clearPending();
        inFlightCount.set(0);
        this.receiveMaximum = normalizeReceiveMaximum(newReceiveMaximum);
    }

    public void clearPending() {
        MqttPublishMessage message;
        while ((message = pendingMessages.poll()) != null) {
            message.release();
        }
    }

    public void removeMetrics(MeterRegistry registry) {
        registry.remove(queueSizeGauge);
    }

    public AtomicInteger inFlightCount() {
        return inFlightCount;
    }

    public Queue<MqttPublishMessage> pendingMessages() {
        return pendingMessages;
    }

}
