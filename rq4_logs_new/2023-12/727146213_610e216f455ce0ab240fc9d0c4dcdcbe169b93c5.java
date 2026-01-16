package com.example.fpga;

import com.example.fpga.client.FPGAMockClient;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;

public class ClientApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientApp.class);

    public static void main(String[] args) throws Exception {

        LOGGER.info("Creating a new FPGA UDP Client");

        String host = Optional.ofNullable(System.getenv("FPGA_CLIENT_HOST"))
                .orElse("127.0.0.1");
        int port = Optional.ofNullable(System.getenv("FPGA_CLIENT_PORT"))
                .map(Integer::parseInt)
                .orElse(53060);
        int count = Optional.ofNullable(System.getenv("RECORD_COUNT"))
                .map(Integer::parseInt)
                .orElse(-1);
        int interval = Optional.ofNullable(System.getenv("RECORD_INTERVAL"))
                .map(Integer::parseInt)
                .orElse(3000);
        int timeSampleSize = Optional.ofNullable(System.getenv("TIME_SAMPLE_SIZE"))
                .map(Integer::parseInt)
                .orElse(2048);

        try (FPGAMockClient client = FPGAMockClient.builder().port(port).build()) {

            ChannelFuture channelFuture = client.startup(host);

            LOGGER.info("A new FPGA Mock Client is created, [{}:{}, iterator:{}, interval:{}]",
                    host, port, count, interval);

            for (long index = count; index != 0; index--) {
                if (channelFuture.isSuccess()) {
                    channelFuture.channel()
                            .writeAndFlush(
                                    Unpooled.wrappedBuffer(randomRecord(timeSampleSize))
                            );
                }
                Thread.sleep(interval);
            }
        }
    }

    private static byte[] randomRecord(int timeSampleSize) {
        Random random = new Random();
        byte[] resultArray = new byte[12 + timeSampleSize * 2];
        ByteBuffer byteBuffer = ByteBuffer.wrap(resultArray);

        short channelId = (short) random.nextInt(1000);
        byteBuffer.put(ByteBuffer.allocate(2).putShort(channelId).array());

        short antennaId = (short) random.nextInt(224);
        byteBuffer.put(ByteBuffer.allocate(2).putShort(antennaId).array());

        long counter = Math.abs(random.nextLong());
        byteBuffer.put(ByteBuffer.allocate(8).putLong(counter).array());

        LOGGER.info("Sent channelId:{}, antennaId:{}, counter:{} ", channelId, antennaId, counter);

        byte[] data = new byte[timeSampleSize * 2];
        random.nextBytes(data);
        byteBuffer.put(data);

        return resultArray;
    }
}