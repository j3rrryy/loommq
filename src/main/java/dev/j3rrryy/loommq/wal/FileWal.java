package dev.j3rrryy.loommq.wal;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public final class FileWal implements Wal {

    private static final int HEADER_SIZE = 4;
    private static final int MAX_RECORD_SIZE = 10 * 1024 * 1024;

    private final FileChannel channel;
    private final ReentrantLock writeLock = new ReentrantLock();
    private long nextOffset = 0;
    private long writePosition = 0;

    public FileWal(Path path) {
        try {
            boolean exists = Files.exists(path);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (exists) {
                recover();
            }
            log.info("WAL initialized at {}, next offset: {}", path, nextOffset);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open WAL: " + path, e);
        }
    }

    @Override
    public void append(byte[] payload) {
        if (payload.length > MAX_RECORD_SIZE) {
            throw new IllegalArgumentException("Record too large: " + payload.length);
        }
        writeLock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length);
            buffer.putInt(payload.length);
            buffer.put(payload);
            buffer.flip();

            while (buffer.hasRemaining()) {
                writePosition += channel.write(buffer, writePosition);
            }
            channel.force(false);
            nextOffset++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append to WAL", e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public byte[] read(long offset) {
        if (offset >= nextOffset) {
            return null;
        }
        try {
            long currentOffset = 0;
            long currentPosition = 0;
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

            while (currentOffset < offset) {
                currentPosition += readFully(header, currentPosition);
                header.flip();
                int length = header.getInt();
                currentPosition += length;
                currentOffset++;
                header.clear();
            }

            currentPosition += readFully(header, currentPosition);
            header.flip();
            int length = header.getInt();

            ByteBuffer payload = ByteBuffer.allocate(length);
            readFully(payload, currentPosition);

            return payload.array();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read from WAL", e);
        }
    }

    @Override
    public long nextOffset() {
        return nextOffset;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Failed to close WAL channel", e);
        }
    }

    private void recover() throws IOException {
        long currentPosition = 0;
        long fileSize = channel.size();
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

        while (currentPosition < fileSize) {
            header.clear();
            int read = channel.read(header, currentPosition);
            if (read < HEADER_SIZE) {
                break;
            }
            header.flip();
            int length = header.getInt();

            if (currentPosition + HEADER_SIZE + length > fileSize) {
                break;
            }

            currentPosition += HEADER_SIZE + length;
            nextOffset++;
        }

        if (currentPosition < fileSize) {
            log.warn("Truncating WAL from {} to {} bytes due to incomplete record", fileSize, currentPosition);
            channel.truncate(currentPosition);
        }
        writePosition = currentPosition;
    }

    private int readFully(ByteBuffer buffer, long position) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + totalRead);
            if (read < 0) {
                throw new IOException("Unexpected EOF");
            }
            totalRead += read;
        }
        return totalRead;
    }

}
