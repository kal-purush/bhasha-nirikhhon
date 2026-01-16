package com.my.android;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;

/**
 * Pc端与Android的通信, 通过:
 * adb forward tcp:10086 tcp:10086
 */
public class SocketClientUtils {
    /**
     * 端口号
     */
    private static final int PHONE_PORT = 10086;
    public static final String HOST = "localhost";


    public static void main(String[] args) {
        try {
            Socket socket = new Socket(HOST, PHONE_PORT);
            //读取服务器端数据
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //向服务器端发送数据
            PrintStream out = new PrintStream(socket.getOutputStream());
            System.out.print("请输入: \t");
//            String str = new BufferedReader(new InputStreamReader(System.in)).readLine();
            String str = "test";
            out.println(str);

            String ret = input.readLine();
            System.out.println("服务器端返回过来的是: " + ret);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


}