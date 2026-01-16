package net.tch.java.TCPDemo;

import java.io.*;
import java.net.Socket;

/**
 * @description:
 * @auth tongchenghao
 * @date 2020/2/9
 */
public class ClientDemo {

    public static void main(String[] args) {
        Socket socket = null;
        OutputStream outputStream = null;
        try {
            socket = new Socket("127.0.0.1",8080);
            outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream,true);
            printWriter.println("tch\n啦啦啦");
            printWriter.println("123");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (socket!=null){
                    socket.close();
                }
                if(outputStream!=null){
                    outputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}