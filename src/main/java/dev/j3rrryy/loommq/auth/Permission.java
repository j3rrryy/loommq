package dev.j3rrryy.loommq.auth;

import java.util.Locale;

public record Permission(Action action, String topic) {

    public enum Action {
        PUBLISH, SUBSCRIBE, ALL;

        public static Action parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "publish" -> PUBLISH;
                case "subscribe" -> SUBSCRIBE;
                case "*", "all" -> ALL;
                default -> throw new IllegalArgumentException("Unknown action: " + value);
            };
        }
    }

}
