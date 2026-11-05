package dev.j3rrryy.loommq.broker.topic;

import java.nio.charset.StandardCharsets;

public final class TopicValidator {

    private static final int MAX_LENGTH_BYTES = 65535;

    public static boolean isValid(String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        if (topic.indexOf('\u0000') >= 0) {
            return false;
        }
        if (topic.contains("+") || topic.contains("#")) {
            return false;
        }
        if (topic.startsWith("$share/")) {
            return false;
        }
        return topic.getBytes(StandardCharsets.UTF_8).length <= MAX_LENGTH_BYTES;
    }

}
