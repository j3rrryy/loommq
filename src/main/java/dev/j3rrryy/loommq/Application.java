package dev.j3rrryy.loommq;

import dev.j3rrryy.loommq.auth.ConnectionRateLimiter;
import dev.j3rrryy.loommq.auth.UserStore;
import dev.j3rrryy.loommq.auth.YamlUserStore;
import dev.j3rrryy.loommq.broker.SubscriptionRegistry;
import dev.j3rrryy.loommq.config.Config;
import dev.j3rrryy.loommq.config.ConfigLoader;
import dev.j3rrryy.loommq.server.MqttServer;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class Application implements AutoCloseable {

    private final MqttServer server;
    private final ConnectionRateLimiter rateLimiter;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Application(Path configPath) {
        Config config = new ConfigLoader().load(configPath);
        UserStore userStore = new YamlUserStore(config.usersFile());
        this.rateLimiter = new ConnectionRateLimiter(config.rateLimit());
        SubscriptionRegistry registry = new SubscriptionRegistry();
        this.server = new MqttServer(config, userStore, rateLimiter, registry);
    }

    public int run() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "loommq-shutdown"));
        log.info("Starting LoomMQ");
        server.start();
        server.awaitShutdown();
        return 0;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            server.stop();
            rateLimiter.close();
        }
    }

}
