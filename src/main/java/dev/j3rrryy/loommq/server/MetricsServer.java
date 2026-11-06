package dev.j3rrryy.loommq.server;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public final class MetricsServer {

    private static final String METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final PrometheusMeterRegistry meterRegistry;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public MetricsServer(PrometheusMeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
            serverChannel.close().syncUninterruptibly();
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
            if (HttpMethod.GET.equals(request.method()) && "/metrics".equals(request.uri())) {
                sendResponse(ctx, request, HttpResponseStatus.OK, meterRegistry.scrape(), METRICS_CONTENT_TYPE);
            } else {
                sendResponse(ctx, request, HttpResponseStatus.NOT_FOUND, "Not Found", HttpHeaderValues.TEXT_PLAIN.toString());
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
            ctx.writeAndFlush(response).addListener(_ -> {
                if (!keepAlive) {
                    ctx.close();
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

    }

}
