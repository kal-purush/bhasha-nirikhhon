package com.huiyuan2.cloud.basic.network;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.MDC;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @description:
 * @author： 灰原二
 * @date: 2022/9/26 22:47
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class AbstractChannelHandler extends ChannelInboundHandlerAdapter {

    // inbound handler 专门用来负责处理接收到的数据，设置packet type类型集合
    private Set<Integer> intersetPackageTypes;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Executor executor = getExecutor();
        if(executor != null){
            executor.execute(()->channelreadInternal(ctx,msg));
        }else {
            //如果没有线程池，就直接自己跑
            channelreadInternal(ctx,msg);
        }
    }

    private void channelreadInternal(ChannelHandlerContext ctx, Object msg){
        try{
            String loggerId = DigestUtils.md5Hex("" + System.nanoTime() + new Random().nextInt());
            MDC.put("logger_id",loggerId);

            NettyPacket nettyPacket = (NettyPacket)msg;
            boolean consumedMsg = false;
            if(!getPackageTypes().isEmpty()||getPackageTypes().contains(nettyPacket.getPacketType())){
                try{
                    consumedMsg = handlePackage(ctx, nettyPacket);
                }catch (Exception e){
                    log.info("处理请求发生异常：",e);
                }
            }
            if(!consumedMsg){
                ctx.fireChannelRead(msg);
            }
        }finally {
            MDC.remove("logger_id");
        }
    }

    /**
     * 处理网络包
     * @param ctx 上下文
     * @param nettyPacket 网络包
     * @return 是否消费了信息
     * @throws Exception
     */
    protected abstract boolean handlePackage(ChannelHandlerContext ctx,NettyPacket nettyPacket) throws Exception;

    protected Executor getExecutor(){
        return null;
    }

    private Set<Integer> getPackageTypes(){
        if(intersetPackageTypes == null){
            intersetPackageTypes = intersetPackageTypes();
        }
        return intersetPackageTypes;
    }

    /**
     * 感兴趣的消息类型 集合
     * @return
     */
    protected abstract Set<Integer> intersetPackageTypes();
}