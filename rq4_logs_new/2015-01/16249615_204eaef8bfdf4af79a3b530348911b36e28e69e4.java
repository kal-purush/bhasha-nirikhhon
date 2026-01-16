package org.sevenasix.medium.application;

import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.net.URLConnection;

@ComponentScan
@EnableAutoConfiguration
public class RestTestDriver{

    boolean connected = false;

    @Before
    public void run(String[] args){
        if(!connected){
            SpringApplication.run(RestTestDriver.class, args);
            connected = true;
        }
    }

    @Test
    public void checkNameExists() throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        URL url = new URL("http://localhost:8080");
        URLConnection conn = url.openConnection();
        conn.connect();

        ReadableByteChannel byteChannel = Channels.newChannel(conn.getInputStream());

        StringBuilder stringBuilder = new StringBuilder();
        int bytesReadCount = 0;
        while((bytesReadCount = byteChannel.read(buffer)) > 0){
            byte[] data = new byte[bytesReadCount];
            System.arraycopy(buffer.array(), 0, data, 0, bytesReadCount);
            buffer.clear();
            stringBuilder.append(new String(data));

        }

    



    }

}