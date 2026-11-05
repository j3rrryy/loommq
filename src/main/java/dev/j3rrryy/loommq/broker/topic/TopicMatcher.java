package dev.j3rrryy.loommq.broker.topic;

public final class TopicMatcher {

    public static boolean matches(String pattern, String topic) {
        String[] p = pattern.split("/", -1);
        String[] t = topic.split("/", -1);
        int i = 0;
        for (; i < p.length; i++) {
            if (p[i].equals("#")) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!p[i].equals("+") && !p[i].equals(t[i])) {
                return false;
            }
        }
        return i == t.length;
    }

}
