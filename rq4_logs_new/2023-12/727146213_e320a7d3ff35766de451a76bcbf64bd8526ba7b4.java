package com.example.flink.source.handler;

import com.example.flink.data.SampleData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Builder
@AllArgsConstructor
public class SampleDataHandler extends SimpleChannelInboundHandler<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataHandler.class);

    private SourceFunction.SourceContext<SampleData> sourceContext;

    @Builder.Default
    private int timeSampleSize = 2048;

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) throws Exception {
        ByteBuffer channelId = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, 2));
        ByteBuffer antennaId = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 2, 4));
        ByteBuffer counter = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 4, 12));

        ByteBuffer realArray = ByteBuffer.wrap(new byte[timeSampleSize]);
        ByteBuffer imaginaryArray = ByteBuffer.wrap(new byte[timeSampleSize]);
        for (int i = 12; i < bytes.length; i++){
            if (i % 2 == 0){
                realArray.put(bytes[i]);
            }else {
                imaginaryArray.put(bytes[i]);
            }
        }

        SampleData sampleData = SampleData.builder()
                .channelId((int) channelId.getShort())
                .antennaId((int) antennaId.getShort())
                .startCounter(counter.getLong())
                .realArray(realArray.array())
                .imaginaryArray(imaginaryArray.array())
                .build();

        LOGGER.info("Message received : " + sampleData);
        if (null != sourceContext) {
            sourceContext.collect(sampleData);
        }
    }
}