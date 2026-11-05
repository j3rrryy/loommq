package dev.j3rrryy.loommq.broker;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

public record RetainedMessage(ByteBuf payload, int qos, long expiresAt) implements ReferenceCounted {

    @Override
    public int refCnt() {
        return payload.refCnt();
    }

    @Override
    public RetainedMessage retain() {
        payload.retain();
        return this;
    }

    @Override
    public RetainedMessage retain(int increment) {
        payload.retain(increment);
        return this;
    }

    @Override
    public RetainedMessage touch() {
        payload.touch();
        return this;
    }

    @Override
    public RetainedMessage touch(Object hint) {
        payload.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return payload.release();
    }

    @Override
    public boolean release(int decrement) {
        return payload.release(decrement);
    }

}
