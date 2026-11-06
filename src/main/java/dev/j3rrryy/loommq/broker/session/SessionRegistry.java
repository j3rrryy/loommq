package dev.j3rrryy.loommq.broker.session;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ConcurrentHashMap;

public final class SessionRegistry {

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    public SessionRegistry(MeterRegistry registry) {
        Gauge.builder("loommq.sessions.active", sessions, ConcurrentHashMap::size).register(registry);
    }

    public Session get(String clientId) {
        return sessions.get(clientId);
    }

    public void put(Session session) {
        sessions.put(session.clientId(), session);
    }

    public void remove(String clientId) {
        sessions.remove(clientId);
    }

}
