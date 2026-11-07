package dev.j3rrryy.loommq.config;

import java.nio.file.Path;

public record Config(
        String host,
        int port,
        int metricsPort,
        Path usersFile,
        Path walFile,
        RateLimitConfig rateLimit
) {

    public static Config defaults() {
        return new Config(
                "0.0.0.0",
                1883,
                8080,
                Path.of("users.yaml"),
                Path.of("data/wal.log"),
                new RateLimitConfig(5, 60, 300)
        );
    }

    public record RateLimitConfig(int maxAttempts, int windowSeconds, int banSeconds) {

    }

}
