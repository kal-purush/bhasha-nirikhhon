package sap.gb.cloud.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * Это будет реализоваться и исправляться если клиент заработает через netty
 */

public class ServerHandler extends ChannelDuplexHandler {
    Scanner scanner = new Scanner(System.in);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("ClientActive");
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        String s = scanner.nextLine();
        byte[] stringBytes = "toServer".getBytes();
        ByteBuf byteBuf = ctx.alloc().buffer(stringBytes.length);
        byteBuf.writeBytes(stringBytes);
        ctx.writeAndFlush(byteBuf);
        //byteBuf.release();
        System.out.println(remoteAddress);
        System.out.println(localAddress);

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        byte[] bytes = new byte[8];
        byteBuf.readBytes(bytes);
        String result = new String(bytes); //service.execute(byteBuf);
        byteBuf.release();
        System.out.println(result);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        System.out.println("i in write method from Client");
        String s = (String) msg;
        System.out.println(s);
        byte[] stringBytes = s.getBytes();
        ByteBuf byteBuf = ctx.alloc().buffer(stringBytes.length);
        byteBuf.writeBytes(stringBytes);
        ctx.writeAndFlush(byteBuf);
        byteBuf.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}