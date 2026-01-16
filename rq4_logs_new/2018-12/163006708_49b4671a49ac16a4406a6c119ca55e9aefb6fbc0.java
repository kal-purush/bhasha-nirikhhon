package com.demo.jdk_proxy;





import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;


/**
 * 动态代理
 * 动态代理实现主要是实现InvocationHandler，并且将目标对象注入到代理对象中，利用反射机制来执行目标对象的方法.
 *
 *  代理类的特点：
 *  * 1.和目标类有共同的接口
 *  * 2.同时拥有目标类的对象和切面类的对象。调用目标类执行目标方法，切面类对象调用切面方法，比如前置通知和后置通知。
 */
public class DynaHelloProxy implements InvocationHandler {
    //目标对象
    private Object target;
    //调用对象(需要在目标对象执行前后执行方法的对象，比如切面类的对象)
    private Object proxy ;
    public Object bind(Object target){
        this.target = target;
        return Proxy.newProxyInstance(target.getClass().getClassLoader(),target.getClass().getInterfaces(),this);
    }
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("调用开始");
        //调用目标类的方法
        Object obj = method.invoke(this.target,args);
        System.out.println("调用结束");
        return obj;
    }
}