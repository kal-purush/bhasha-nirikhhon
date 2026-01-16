package io.netty.handler.codec.msgpack;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.msgpack.MessagePack;

import java.util.List;

/**
 * TODO
 * Created By: Robin Yang
 * Created At: 2016-01-18 16:22
 */
public class MsgpackDecoder extends MessageToMessageDecoder<ByteBuf>{
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        final int length = msg.readableBytes();
        final byte[] array = new byte[length];

        msg.getBytes(msg.readerIndex(), array, 0, length);

        MessagePack msgPack = new MessagePack();

        out.add(msgPack.read(array));
    }
}