package com.ddkangfu;

import com.ddkangfu.intercept.MyInterceptor;
import com.ddkangfu.proxy.ProxyBean;
import com.ddkangfu.service.HelloService;
import com.ddkangfu.service.impl.HelloServiceImpl;

public class Application {
    public static void main(String[] args) {
        HelloService helloService = new HelloServiceImpl();
        HelloService proxy = (HelloService) ProxyBean.getProxyBean(helloService, new MyInterceptor());
        proxy.sayHello("ZhangSan");
        System.out.println("\n################# name is null! ###############");
        proxy.sayHello(null);
        System.out.println("\n################# name is null! ###############");
        proxy.laugh();
    }
}