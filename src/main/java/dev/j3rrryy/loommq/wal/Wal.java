package dev.j3rrryy.loommq.wal;

public interface Wal extends AutoCloseable {

    void append(byte[] payload);

    byte[] read(long offset);

    long nextOffset();

    @Override
    void close();

}
