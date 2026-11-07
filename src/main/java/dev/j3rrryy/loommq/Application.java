package dev.j3rrryy.loommq;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import dev.j3rrryy.loommq.auth.ConnectionRateLimiter;
import dev.j3rrryy.loommq.auth.UserStore;
import dev.j3rrryy.loommq.auth.YamlUserStore;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.config.Config;
import dev.j3rrryy.loommq.config.ConfigLoader;
import dev.j3rrryy.loommq.server.MetricsServer;
import dev.j3rrryy.loommq.server.MqttServer;
import dev.j3rrryy.loommq.wal.FileWal;
import dev.j3rrryy.loommq.wal.Wal;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Application implements AutoCloseable {

    private final MqttServer server;
    private final ConnectionRateLimiter rateLimiter;
    private final Wal wal;
    private final MetricsServer metricsServer;
    private final JvmGcMetrics jvmGcMetrics;
    private final Config config;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Application(Path configPath) {
        this.config = new ConfigLoader().load(configPath);
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        AtomicInteger activeConnections = new AtomicInteger(0);
        AtomicBoolean isReady = new AtomicBoolean(false);

        this.jvmGcMetrics = new JvmGcMetrics();
        this.jvmGcMetrics.bindTo(meterRegistry);

        new ClassLoaderMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);

        Gauge.builder("loommq.connections.active", activeConnections, AtomicInteger::get).register(meterRegistry);

        UserStore userStore = new YamlUserStore(config.usersFile(), meterRegistry);
        this.rateLimiter = new ConnectionRateLimiter(config.rateLimit());
        SubscriptionRegistry registry = new SubscriptionRegistry(meterRegistry);
        SessionRegistry sessionRegistry = new SessionRegistry(meterRegistry);
        this.wal = new FileWal(config.walFile(), meterRegistry);

        this.server = new MqttServer(config, userStore, rateLimiter, registry, wal, sessionRegistry,
                meterRegistry, activeConnections, isReady);
        this.metricsServer = new MetricsServer(meterRegistry, isReady);
    }

    public int run() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "loommq-shutdown"));
        try {
            log.info("Starting LoomMQ");
            metricsServer.start(config.host(), config.metricsPort());
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
            metricsServer.stop();
            rateLimiter.close();
            wal.close();
            jvmGcMetrics.close();
        }
    }

}
