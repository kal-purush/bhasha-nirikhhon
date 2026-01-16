package com.example.utils.beans.factory;

import com.example.logging.LoggerService;

import java.lang.reflect.Proxy;

public class ProxyFactory {
    public static Object createProxy(Object target, LoggerService logger) {
        return Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                target.getClass().getInterfaces(),
                (proxy, method, args) -> {
                    logger.log("Before method: " + method.getName());
                    Object result = method.invoke(target, args);
                    logger.log("After method: " + method.getName());
                    return result;
                });
    }
}