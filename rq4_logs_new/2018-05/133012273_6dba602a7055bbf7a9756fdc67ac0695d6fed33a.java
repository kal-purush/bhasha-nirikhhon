package com.springregister;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ServiceProxyHandler implements InvocationHandler {

	public Object create(Class<?> interfaceClass) {
		return Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] { interfaceClass }, this);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		// call remote

		return "Hello world!";
	}

}