package dev.j3rrryy.loommq.broker.session;

import io.netty.util.AttributeKey;

public record WillMessage(String topic, byte[] payload, int qos, boolean retain) {

    public static final AttributeKey<WillMessage> WILL_KEY = AttributeKey.valueOf("loommq.will");

}
