package dev.j3rrryy.loommq.server;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

public final class MetricsServer {

    private static final String METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final PrometheusMeterRegistry meterRegistry;
    private final AtomicBoolean isReady;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public MetricsServer(PrometheusMeterRegistry meterRegistry, AtomicBoolean isReady) {
        this.meterRegistry = meterRegistry;
        this.isReady = isReady;
    }

    private static void shutdownGroup(EventLoopGroup group) {
        if (group != null) {
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    public void start(int port) throws InterruptedException {
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(65536));
                        ch.pipeline().addLast(new MetricsHandler());
                    }
                });
        serverChannel = bootstrap.bind(port).sync().channel();
    }

    public void stop() {
        if (serverChannel != null) {
            var _ = serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        shutdownGroup(bossGroup);
        shutdownGroup(workerGroup);
        bossGroup = null;
        workerGroup = null;
    }

    private final class MetricsHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            if (!HttpMethod.GET.equals(request.method())) {
                sendResponse(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED,
                        "Method Not Allowed", HttpHeaderValues.TEXT_PLAIN.toString());
                return;
            }

            String uri = request.uri();
            switch (uri) {
                case "/metrics" -> sendResponse(ctx, request, HttpResponseStatus.OK,
                        meterRegistry.scrape(), METRICS_CONTENT_TYPE);
                case "/health/live" -> sendResponse(ctx, request, HttpResponseStatus.OK,
                        "OK", HttpHeaderValues.TEXT_PLAIN.toString());
                case "/health/ready" -> {
                    if (isReady.get()) {
                        sendResponse(ctx, request, HttpResponseStatus.OK,
                                "OK", HttpHeaderValues.TEXT_PLAIN.toString());
                    } else {
                        sendResponse(ctx, request, HttpResponseStatus.SERVICE_UNAVAILABLE,
                                "Not Ready", HttpHeaderValues.TEXT_PLAIN.toString());
                    }
                }
                case null, default -> sendResponse(ctx, request, HttpResponseStatus.NOT_FOUND,
                        "Not Found", HttpHeaderValues.TEXT_PLAIN.toString());
            }
        }

        private void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status,
                                  String content, String contentType) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(content, StandardCharsets.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            HttpUtil.setContentLength(response, response.content().readableBytes());
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            HttpUtil.setKeepAlive(response, keepAlive);
            var _ = ctx.writeAndFlush(response).addListener(_ -> {
                if (!keepAlive) {
                    var _ = ctx.close();
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            var _ = ctx.close();
        }

    }

}
