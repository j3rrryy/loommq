package dev.j3rrryy.loommq.auth;

import dev.j3rrryy.loommq.config.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public final class ConnectionRateLimiter implements AutoCloseable {

    private final int maxAttempts;
    private final long windowMillis;
    private final long banMillis;
    private final ConcurrentHashMap<String, AttemptState> attempts = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner;

    public ConnectionRateLimiter(Config.RateLimitConfig config) {
        this.maxAttempts = config.maxAttempts();
        this.windowMillis = TimeUnit.SECONDS.toMillis(config.windowSeconds());
        this.banMillis = TimeUnit.SECONDS.toMillis(config.banSeconds());
        this.cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rate-limiter-cleaner");
            t.setDaemon(true);
            return t;
        });
        this.cleaner.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    public boolean isAllowed(String ip) {
        AttemptState state = attempts.get(ip);
        if (state == null) {
            return true;
        }
        long now = System.currentTimeMillis();
        if (state.bannedUntil > now) {
            return false;
        }
        if (now - state.lastAttemptTime > windowMillis) {
            return true;
        }
        return state.count < maxAttempts;
    }

    public void recordFailure(String ip) {
        long now = System.currentTimeMillis();
        attempts.compute(ip, (_, state) -> {
            if (state == null) {
                return new AttemptState(1, now, 0);
            }
            if (state.bannedUntil > now) {
                return state;
            }
            if (now - state.lastAttemptTime > windowMillis) {
                return new AttemptState(1, now, 0);
            }
            int newCount = state.count + 1;
            long bannedUntil = newCount >= maxAttempts ? now + banMillis : 0;
            if (bannedUntil > 0) {
                log.warn("IP {} temporarily banned due to too many failed attempts", ip);
            }
            return new AttemptState(newCount, now, bannedUntil);
        });
    }

    public void recordSuccess(String ip) {
        attempts.remove(ip);
    }

    private void cleanup() {
        long now = System.currentTimeMillis();
        attempts.entrySet().removeIf(entry -> {
            AttemptState state = entry.getValue();
            return state.bannedUntil < now && (now - state.lastAttemptTime > windowMillis);
        });
    }

    @Override
    public void close() {
        cleaner.shutdownNow();
    }

    private record AttemptState(int count, long lastAttemptTime, long bannedUntil) {

    }

}
