package dev.j3rrryy.loommq.auth;

import java.util.List;

import dev.j3rrryy.loommq.broker.topic.TopicMatcher;

public record User(String username, String passwordHash, List<Permission> permissions) {

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean canPublish(String topic) {
        return hasPermission(Permission.Action.PUBLISH, topic);
    }

    public boolean canSubscribe(String topic) {
        return hasPermission(Permission.Action.SUBSCRIBE, topic);
    }

    private boolean hasPermission(Permission.Action action, String topic) {
        return permissions.stream().anyMatch(p ->
                (p.action() == action || p.action() == Permission.Action.ALL)
                        && TopicMatcher.matches(p.topic(), topic));
    }

    @Override
    public String toString() {
        return "User[username=" + username + ", permissions=" + permissions.size() + "]";
    }

}
