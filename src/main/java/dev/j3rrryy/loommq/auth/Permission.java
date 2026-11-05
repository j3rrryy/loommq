package dev.j3rrryy.loommq.auth;

public record Permission(Action action, String topic) {

    public enum Action {
        PUBLISH, SUBSCRIBE, ALL;

        public static Action parse(String value) {
            return switch (value.toLowerCase()) {
                case "publish" -> PUBLISH;
                case "subscribe" -> SUBSCRIBE;
                case "*", "all" -> ALL;
                default -> throw new IllegalArgumentException("Unknown action: " + value);
            };
        }
    }

}
