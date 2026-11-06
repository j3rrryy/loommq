package dev.j3rrryy.loommq.broker.wal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record WalMessage(String topic, byte[] payload) {

    public static WalMessage fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);

        return new WalMessage(topic, payload);
    }

    public byte[] toBytes() {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + payload.length);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.put(payload);
        return buffer.array();
    }

}
