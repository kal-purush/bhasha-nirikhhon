package org.smart4j.chapter4.cglib;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.smart4j.chapter4.Hello;
import org.smart4j.chapter4.HelloImpl;

import java.lang.reflect.Method;

/**
 * @description:CGlib动态代理
 * @author:shang
 * @date: Created in 2019/12/19 0019 9:50
 * @version: 1.0.0
 * @modified By:
 */
public class CGlibProxy implements MethodInterceptor {

    private static CGlibProxy instance = new CGlibProxy();

    private CGlibProxy(){

    }

    private static CGlibProxy getInstance(){
        return instance;
    }

    public <T> T getProxy(Class<T> cls){
        return (T) Enhancer.create(cls,this);
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        before();
        Object result = methodProxy.invokeSuper(o,objects);
        after();
        return result;
    }

    private void before(){
        System.out.println("before");
    }

    private void after(){
        System.out.println("after");;
    }

    public static void main(String[] args) {
        Hello hello = CGlibProxy.getInstance().getProxy(HelloImpl.class);
        hello.sya("Json");
    }
}