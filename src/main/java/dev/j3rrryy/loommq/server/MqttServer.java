package dev.j3rrryy.loommq.server;

import dev.j3rrryy.loommq.auth.ConnectionRateLimiter;
import dev.j3rrryy.loommq.auth.UserStore;
import dev.j3rrryy.loommq.broker.routing.SubscriptionRegistry;
import dev.j3rrryy.loommq.broker.session.SessionRegistry;
import dev.j3rrryy.loommq.config.Config;
import dev.j3rrryy.loommq.wal.Wal;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RequiredArgsConstructor
public final class MqttServer {

    private final Config config;
    private final UserStore userStore;
    private final ConnectionRateLimiter rateLimiter;
    private final SubscriptionRegistry registry;
    private final Wal wal;
    private final SessionRegistry sessionRegistry;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeConnections;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private static void shutdownGroup(EventLoopGroup group) {
        if (group != null) {
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static EventLoopGroup newEventLoopGroup(int threads) {
        if (Epoll.isAvailable()) {
            return new MultiThreadIoEventLoopGroup(threads, EpollIoHandler.newFactory());
        }
        if (KQueue.isAvailable()) {
            return new MultiThreadIoEventLoopGroup(threads, KQueueIoHandler.newFactory());
        }
        return new MultiThreadIoEventLoopGroup(threads, NioIoHandler.newFactory());
    }

    private static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    public void start() throws InterruptedException {
        bossGroup = newEventLoopGroup(1);
        workerGroup = newEventLoopGroup(Runtime.getRuntime().availableProcessors());

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(serverSocketChannelClass())
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new MqttChannelInitializer(userStore, rateLimiter, registry, sessionRegistry, wal,
                        meterRegistry, activeConnections));

        serverChannel = bootstrap.bind(config.host(), config.port()).sync().channel();
        log.info("LoomMQ listening on {}:{}", config.host(), config.port());
    }

    public void awaitShutdown() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    public void stop() {
        log.info("Stopping LoomMQ");
        closeServerChannel();
        shutdownGroup(bossGroup);
        shutdownGroup(workerGroup);
        log.info("LoomMQ stopped");
    }

    private void closeServerChannel() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
    }

}
