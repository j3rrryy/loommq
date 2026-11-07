package dev.j3rrryy.loommq.broker.wal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public record WalMessage(String topic, ByteBuf payload) {

    public static WalMessage fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);

        return new WalMessage(topic, Unpooled.wrappedBuffer(payloadBytes));
    }

    public byte[] toBytes() {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + payload.readableBytes());
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        payload.getBytes(payload.readerIndex(), buffer);
        return buffer.array();
    }

}
