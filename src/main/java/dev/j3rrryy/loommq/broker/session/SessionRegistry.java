package dev.j3rrryy.loommq.broker.session;

import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public final class SessionRegistry {

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final MeterRegistry registry;

    public SessionRegistry(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("loommq.sessions.active", sessions, ConcurrentHashMap::size).register(registry);
    }

    public Session get(String clientId) {
        return sessions.get(clientId);
    }

    public void put(Session session) {
        sessions.put(session.clientId(), session);
    }

    public void remove(String clientId) {
        Session session = sessions.remove(clientId);
        if (session != null) {
            session.clearPending();
            session.removeMetrics(registry);
        }
    }

    public void remove(String clientId, Session session) {
        if (sessions.remove(clientId, session)) {
            session.clearPending();
            session.removeMetrics(registry);
        }
    }

}
