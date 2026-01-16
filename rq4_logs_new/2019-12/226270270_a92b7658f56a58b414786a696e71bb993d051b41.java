package com.gxl.study.nio.aio;

import com.gxl.study.nio.aio.client.AIOClient2;
import com.gxl.study.nio.aio.server.AIOServer2;

import java.util.Scanner;

/**
 * 测试方法
 * 有个缺点，代码不支持多线程
 * @author https://blog.csdn.net/anxpp/article/details/51512200
 * @version 1.0
 */
public class AIO2Test {
    //测试主方法
    //也可以分别去启动
    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        //运行服务器
        AIOServer2.start();
        //避免客户端先于服务器启动前执行代码
        Thread.sleep(100);
        //运行客户端
        AIOClient2.start();
        System.out.println("请输入请求消息：");
        Scanner scanner = new Scanner(System.in);
        while (AIOClient2.sendMsg(scanner.nextLine())) ;
    }
}