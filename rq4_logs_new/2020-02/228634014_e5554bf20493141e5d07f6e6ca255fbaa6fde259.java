package net.tch.java.RPC.myRPC;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @description:代理类中实际的处理请求的类
 * @auth tongchenghao
 * @date 2020/2/13
 */
public class MyRpcClientInvocationHandler implements InvocationHandler {

    private String host;

    private int port;

    public MyRpcClientInvocationHandler(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 创建传输对象
        MyRpcRequest request = new MyRpcRequest(method.getDeclaringClass().getName(),method.getName(),args);
        // 调用传输层发送请求，并得到远程的返回结果
        MyRpcTCPTransport myRpcTCPTransport = new MyRpcTCPTransport(host,port);
        Object result = myRpcTCPTransport.send(request);
        // 返回结果
        return result;
    }
}