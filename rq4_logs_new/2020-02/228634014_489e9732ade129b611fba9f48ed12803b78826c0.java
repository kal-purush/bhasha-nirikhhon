package net.tch.java.RPC.myRpcByZookeeper;

import net.tch.java.RPC.myRpcByZookeeper.serviceDiscovery.IServiceDiscovery;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @description:代理类中实际的处理请求的类
 * @auth tongchenghao
 * @date 2020/2/13
 */
public class MyRpcClientInvocationHandler implements InvocationHandler {

    IServiceDiscovery serviceDiscovery;

    String version;

    public MyRpcClientInvocationHandler(IServiceDiscovery serviceDiscovery,String version) {
        this.serviceDiscovery = serviceDiscovery;
        this.version = version;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 创建传输对象
        MyRpcRequest request = new MyRpcRequest(method.getDeclaringClass().getName(),method.getName(),version,args);
        // 从zookeeper上获取服务对应的调用地址
        String servicePath = serviceDiscovery.discovery(method.getDeclaringClass().getName());
        // 调用传输层发送请求，并得到远程的返回结果
        MyRpcTCPTransport myRpcTCPTransport = new MyRpcTCPTransport(servicePath);
        Object result = myRpcTCPTransport.send(request);
        // 返回结果
        return result;
    }
}