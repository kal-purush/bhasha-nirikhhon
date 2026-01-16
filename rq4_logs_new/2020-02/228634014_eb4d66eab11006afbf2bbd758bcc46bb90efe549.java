package net.tch.java.RPC.rmi;

import net.tch.java.RPC.IServiceDemo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * @description:RMI客户端，调用RMI发布的服务Demo
 *      RMI是一个纯java的RPC框架
 * @auth tongchenghao
 * @date 2020/2/12
 */
public class RMIClient {
    public static void main(String[] args) {
        try {
            // 通过之前服务绑定的地址，获取到对应的服务
            IServiceDemo serviceDemo = (IServiceDemo) Naming.lookup("rmi://127.0.0.1/ServiceDemo");
            /**
             * 调用服务端的服务，得到返回值，服务过程有服务发布者运行，调用者只能看到服务的输入输出
             */
            String tch = serviceDemo.sayHello("TCH");
            System.out.println(tch);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}