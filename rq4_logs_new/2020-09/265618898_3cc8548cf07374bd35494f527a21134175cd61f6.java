package cn.madf.basicKnowledge.nettyDemo.UdpBrocastDemo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.util.List;

/**
 * 将 DatagramPacket decode 成 NoticeHelpPacket
 * @author 烛影鸾书
 * @date 2020/9/2 14:05
 * @copyright© 2020
 */
public class NoticeDecoder extends MessageToMessageDecoder<DatagramPacket> {

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
        ByteBuf data = msg.content();

        long time = data.readLong();
        int port = data.readInt();
        data.readByte();   // 分隔符
        int idx = data.readerIndex();
        String content = data.slice(idx, data.readableBytes()).toString(CharsetUtil.UTF_8);

        out.add(new NoticeHelpPacket(time, port, content));
    }
}