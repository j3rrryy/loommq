package dev.j3rrryy.loommq.broker;

import java.nio.charset.StandardCharsets;

public final class TopicFilterValidator {

    private static final int MAX_LENGTH_BYTES = 65535;
    private static final String SHARE_PREFIX = "$share/";

    public static boolean isValid(String filter) {
        if (filter == null || filter.isEmpty()) {
            return false;
        }
        if (filter.indexOf('\u0000') >= 0) {
            return false;
        }
        if (filter.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH_BYTES) {
            return false;
        }
        String actual = actualFilter(filter);
        if (actual == null || actual.isEmpty()) {
            return false;
        }
        String[] parts = actual.split("/", -1);
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.contains("#") && (!part.equals("#") || i != parts.length - 1)) {
                return false;
            }
            if (part.contains("+") && !part.equals("+")) {
                return false;
            }
        }
        return true;
    }

    public static String shareGroup(String filter) {
        if (!filter.startsWith(SHARE_PREFIX)) {
            return null;
        }
        int slash = filter.indexOf('/', SHARE_PREFIX.length());
        if (slash < 0) {
            return null;
        }
        return filter.substring(SHARE_PREFIX.length(), slash);
    }

    public static String actualFilter(String filter) {
        if (!filter.startsWith(SHARE_PREFIX)) {
            return filter;
        }
        int slash = filter.indexOf('/', SHARE_PREFIX.length());
        if (slash < 0 || slash == SHARE_PREFIX.length()) {
            return null;
        }
        return filter.substring(slash + 1);
    }

}
