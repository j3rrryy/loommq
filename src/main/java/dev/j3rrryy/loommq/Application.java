package dev.j3rrryy.loommq;

import dev.j3rrryy.loommq.auth.ConnectionRateLimiter;
import dev.j3rrryy.loommq.auth.UserStore;
import dev.j3rrryy.loommq.auth.YamlUserStore;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.config.Config;
import dev.j3rrryy.loommq.config.ConfigLoader;
import dev.j3rrryy.loommq.server.MqttServer;
import dev.j3rrryy.loommq.wal.FileWal;
import dev.j3rrryy.loommq.wal.Wal;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class Application implements AutoCloseable {

    private final MqttServer server;
    private final ConnectionRateLimiter rateLimiter;
    private final Wal wal;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Application(Path configPath) {
        Config config = new ConfigLoader().load(configPath);
        UserStore userStore = new YamlUserStore(config.usersFile());
        this.rateLimiter = new ConnectionRateLimiter(config.rateLimit());
        SubscriptionRegistry registry = new SubscriptionRegistry();
        SessionRegistry sessionRegistry = new SessionRegistry();
        this.wal = new FileWal(Path.of("wal.log"));
        this.server = new MqttServer(config, userStore, rateLimiter, registry, wal, sessionRegistry);
    }

    public int run() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "loommq-shutdown"));
        try {
            log.info("Starting LoomMQ");
            server.start();
            server.awaitShutdown();
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("LoomMQ interrupted, shutting down");
            return 0;
        } catch (Exception e) {
            log.error("Failed to start LoomMQ: {}", e.getMessage());
            return 1;
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            server.stop();
            rateLimiter.close();
            wal.close();
        }
    }

}
