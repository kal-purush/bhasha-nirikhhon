package net.tch.java.RPC;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @description:服务Demo，该服务将被发布，给与其他进程调用，来测试各个rpc框架的功能
 *      1.使用RMI框架
 *           使用RMI框架服务实现必须继承UnicastRemoteObject类
 * @auth tongchenghao
 * @date 2020/2/12
 */
public class ServiceDemo extends UnicastRemoteObject implements IServiceDemo{

    public ServiceDemo() throws RemoteException {
        super();
    }

    @Override
    public String sayHello(String msg) throws RemoteException {
        // 这句输出只会被发布者看到,因为当服务被调用时，实际的运行过程是在发布者中进行，调用者是无法看到输出过程的
        System.out.println("Hello "+msg);
        return "Hello "+msg;
    }
}