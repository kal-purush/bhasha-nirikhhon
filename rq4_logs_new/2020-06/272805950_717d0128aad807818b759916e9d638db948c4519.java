package com.example.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test example of netty to check
 */
public class Main {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 12000;
    private static final AtomicReference<Channel> channelRef = new AtomicReference<>();

    public static void main(String[] args) throws Exception {
        startServer();
        startClient();
        Thread.sleep(500);
        for (int i = 0; i < 10; i++) {
            sendAsync("Hello new message! index " + i, channelRef.get());
        }
        Thread.sleep(500);
    }

    private static void startServer() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        final EventLoopGroup childGroup = new NioEventLoopGroup(5);
        final DefaultEventExecutorGroup workerGroup = new DefaultEventExecutorGroup(5);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(group, childGroup);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.localAddress(new InetSocketAddress(PORT));

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    final ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast("lengthFieldFameDecoder",
                            new LengthFieldBasedFrameDecoder(1024, 0, 2, 0, 2));
                    pipeline.addLast(new LengthFieldPrepender(2));
                    pipeline.addLast(workerGroup, new HelloServerHandler());
                }
            });

            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            channelFuture.channel();
			channelFuture.channel().closeFuture(); // sync
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    private static void startClient() throws InterruptedException {
        try {
            EventLoopGroup group = new NioEventLoopGroup();
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.group(group);
            clientBootstrap.channel(NioSocketChannel.class);
            clientBootstrap.remoteAddress(new InetSocketAddress(HOST, PORT));
            clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    final ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast("lengthFieldFameDecoder",
                            new LengthFieldBasedFrameDecoder(1024, 0, 2, 0, 2));
                    pipeline.addLast(new LengthFieldPrepender(2));
                    pipeline.addLast(new ClientHandler());
                }
            });
            ChannelFuture channelFuture = clientBootstrap.connect().sync();
            channelFuture.addListener(connFuture -> {
                if (!channelFuture.isSuccess()) {
                    return;
                }
                Channel channel = channelFuture.channel();
                setChannel(channel);
            });
        } finally {
		}
    }

    private static void setChannel(Channel channel) {
        channelRef.set(channel);
    }

    public static ChannelFuture sendAsync(String message, Channel channel) throws InterruptedException {
        return channel.writeAndFlush(Unpooled.wrappedBuffer(message.getBytes()));
    }

    public static class HelloServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf inBuffer = (ByteBuf) msg;
            String received = inBuffer.toString(CharsetUtil.UTF_8);
            System.out.println("Server received: " + received + " Thread name " + Thread.currentThread().getName());
            Thread.sleep(1000);
            System.out.println("Server received end: " + received);
        }
    }

    public static class ClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf inBuffer = (ByteBuf) msg;
            String received = inBuffer.toString(CharsetUtil.UTF_8);
            System.out.println("Client received: " + received);
        }
    }
}