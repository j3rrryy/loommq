package dev.j3rrryy.loommq.broker.session;

import java.util.concurrent.ConcurrentHashMap;

public final class Session {

    private final String clientId;
    private final int sessionExpiryInterval;
    private final ConcurrentHashMap<String, Integer> subscriptions = new ConcurrentHashMap<>();

    public Session(String clientId, int sessionExpiryInterval) {
        this.clientId = clientId;
        this.sessionExpiryInterval = sessionExpiryInterval;
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

}
