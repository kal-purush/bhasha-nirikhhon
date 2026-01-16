package client;

import command.Command;
import handlers.ClientInputHandler;
import handlers.ClientOutputHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;


import java.io.File;
import java.net.InetAddress;
import java.nio.file.Paths;

public class Client {
    private SocketChannel channel;

    public Client(InetAddress host, int port) {
        new Thread(() -> {
            EventLoopGroup workerGroup = new NioEventLoopGroup();

            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                channel = ch;
                                ch.pipeline()
                                        .addLast(new StringDecoder(),
                                                new StringEncoder(),
                                                new ClientInputHandler(),
                                                new ClientOutputHandler()
                                        );
                            }
                        });

                // Start the client.
                ChannelFuture future = bootstrap.connect(host, port).sync();

                // Wait until the connection is closed.
                future.channel().closeFuture().sync();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                workerGroup.shutdownGracefully();
            }
        }).start();
    }

    public boolean channelIsReady() {
        return channel != null && channel.isActive();
    }

    public void sendMessage(String message) {
        channel.writeAndFlush(message);
    }

    public void upload(File file, File destination) {
        // TODO: implement.
        String message = String.format("%s %s %s", Command.UPLOAD, file.getPath(), destination.getPath());
        sendMessage(message);
    }

    public void download(File file, File destDir) {
        // TODO: implement.
        String srcPath = file.getPath();
        String destPath = Paths.get(destDir.getPath(), file.getName()).toString();
        String message = String.format("%s %s %s", Command.DOWNLOAD, srcPath, destPath);
        sendMessage(message);
    }

    public void copy(File src, File dest) {
        // TODO: implement.
        String message = String.format("%s %s %s", Command.COPY, src.getPath(), dest.getPath());
        sendMessage(message);
    }

    public void createDirectory(String directoryName) {
        // TODO: implement.
        String message = String.format("%s %s", Command.MKDIR, directoryName);
        sendMessage(message);
    }

    public void grantPermissions(String user, File file, String permissions) {
        // TODO: implement.
        String message = String.format("%s %s %s %s", Command.GRANT_PERMISSIONS, user, file.getPath(), permissions);
        sendMessage(message);
    }

    public void delete(File file) {
        // TODO: implement.
        String message = String.format("%s %s", Command.DELETE, file);
        sendMessage(message);
    }

    public void closeChannel() {
        sendMessage(Command.DISCONNECT);
        channel.close();
    }

    public File getServerDir() {
        // TODO: implement.
        //sendMessage(Command.GET_DIR);
        return null;
    }
}