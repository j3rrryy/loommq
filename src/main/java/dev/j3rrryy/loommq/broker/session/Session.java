package dev.j3rrryy.loommq.broker.session;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class Session {

    private final String clientId;
    private final int sessionExpiryInterval;
    private final ConcurrentHashMap<String, Integer> subscriptions = new ConcurrentHashMap<>();

    private final int receiveMaximum;
    private final AtomicInteger inFlightCount = new AtomicInteger(0);
    private final Queue<MqttPublishMessage> pendingMessages = new ConcurrentLinkedQueue<>();

    public Session(String clientId, int sessionExpiryInterval, int receiveMaximum) {
        this.clientId = clientId;
        this.sessionExpiryInterval = sessionExpiryInterval;
        this.receiveMaximum = receiveMaximum == 0 ? 65535 : receiveMaximum;
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

    public AtomicInteger inFlightCount() {
        return inFlightCount;
    }

    public Queue<MqttPublishMessage> pendingMessages() {
        return pendingMessages;
    }

}
