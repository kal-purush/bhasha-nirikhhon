package com.example.flink.source;

import com.example.flink.CosmicAntennaConf;
import com.example.flink.data.SampleData;
import com.example.flink.source.handler.MessageDecoder;
import com.example.flink.source.handler.SampleDataV2Handler;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.group.ChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.group.DefaultChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.DatagramChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class ServerV2Source extends RichParallelSourceFunction<SampleData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerV2Source.class);
    private static final String BLOCK_HANDLER = "BLOCK-HANDLER";
    private static final String DECODER_IDENTIFIER = "sample-data-decoder";
    private static final String BYTE_DATA_HANDLER_IDENTIFIER = "byte-data-handler";
    private EventLoopGroup eventLoopGroup;
    private ChannelGroup defaultChannelGroup;
    private ChannelId defaultChannelId;

    private int dataHeaderSize;
    private int dataChunkSize;

    @Override
    public void open(Configuration configuration) throws Exception {
        dataHeaderSize = configuration.get(CosmicAntennaConf.DATA_HEADER_SIZE);
        dataChunkSize = configuration.get(CosmicAntennaConf.DATA_CHUNK_SIZE);
        eventLoopGroup = new NioEventLoopGroup();
        defaultChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(eventLoopGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.AUTO_CLOSE, true)
                .option(ChannelOption.RCVBUF_ALLOCATOR,
                        new FixedRecvByteBufAllocator(configuration.get(CosmicAntennaConf.FPGA_PACKAGE_SIZE)))
                .option(ChannelOption.SO_BROADCAST, true);
        serverBootstrap.handler(new ChannelInitializer<DatagramChannel>() {
            @Override
            protected void initChannel(DatagramChannel datagramChannel) throws Exception {
                ChannelPipeline pipeline = datagramChannel.pipeline();
                pipeline.addLast(BLOCK_HANDLER, new MessageToMessageDecoder<>() {
                    @Override
                    protected void decode(ChannelHandlerContext ctx, Object obj, List<Object> out) throws Exception {
                        Thread.currentThread().wait();
                    }
                });
            }
        });
        ChannelFuture channelFuture = serverBootstrap
                .bind(50330)
                .sync();
        LOGGER.info("inner netty server started at {}",
                ((InetSocketAddress) channelFuture.channel().localAddress()).getPort());

        defaultChannelId = channelFuture.channel().id();
        defaultChannelGroup.add(channelFuture.channel());
    }

    @Override
    public void run(SourceContext<SampleData> sourceContext) throws Exception {
        ChannelPipeline channelPipeline = defaultChannelGroup.find(defaultChannelId).pipeline();
        channelPipeline.remove(BLOCK_HANDLER);
        LOGGER.info("inner netty server unregistered the blocking handler");
        channelPipeline.addLast(DECODER_IDENTIFIER, MessageDecoder.builder()
                .dataHeaderSize(dataHeaderSize)
                .dataChunkSize(dataChunkSize)
                .build());
        LOGGER.info("inner netty server registered \"{}\"", DECODER_IDENTIFIER);
        channelPipeline.addLast(BYTE_DATA_HANDLER_IDENTIFIER, SampleDataV2Handler.builder()
                .sourceContext(sourceContext)
                .dataChunkSize(dataChunkSize)
                .build());
        LOGGER.info("inner netty server registered \"{}\"", BYTE_DATA_HANDLER_IDENTIFIER);
        Thread.currentThread().join();
    }

    @Override
    public void cancel() {
        if (null != defaultChannelGroup) {
            defaultChannelGroup.close();
        }
        if (null != eventLoopGroup) {
            eventLoopGroup.shutdownGracefully();
        }
    }
}