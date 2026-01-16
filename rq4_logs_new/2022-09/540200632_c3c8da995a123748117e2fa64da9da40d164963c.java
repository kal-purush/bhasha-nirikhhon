package com.huiyuan2.cloud.basic.network;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: 网络请求
 * @author： 灰原二
 * @date: 2022/9/27 8:04
 */
@Slf4j
public class RequestWrapper{
    private ResponseListener listener;

    private ChannelHandlerContext ctx;

    private NettyPacket request;

    private String requestSequeuece;

    private int nodeId;

    public RequestWrapper(ChannelHandlerContext ctx,NettyPacket request){
        this(ctx,request,-1,null);
    }

    public RequestWrapper( ChannelHandlerContext ctx, NettyPacket request, int nodeId,ResponseListener listener) {
        this.listener = listener;
        this.ctx = ctx;
        this.request = request;
        this.nodeId = nodeId;
//        this.requestSequeuece = reques
    }
}
