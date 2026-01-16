package ru.otus.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import ru.otus.proxy.annotations.Log;

class Ioc {

  private Ioc() {
  }

  static MyClassInterface createMyClass() {
    InvocationHandler handler = new DemoInvocationHandler(new MyClassImpl());
    return (MyClassInterface) Proxy.newProxyInstance(Ioc.class.getClassLoader(),
        new Class<?>[]{MyClassInterface.class}, handler);
  }

  static class DemoInvocationHandler implements InvocationHandler {

    private final MyClassImpl myClass;
    private final Map<Method, Boolean> loggedMethodsCache = new HashMap<>();

    DemoInvocationHandler(MyClassImpl myClass) {
      this.myClass = myClass;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (hasLogger(method)) {
        System.out.printf(">>>Logger: Executed method: %s, param: %s%n", method.getName(), Arrays.toString(args));
      }
      return method.invoke(myClass, args);
    }

    private boolean hasLogger(Method method) throws NoSuchMethodException {
      if (!loggedMethodsCache.containsKey(method)) {
        System.out.println("Cache method Log status for reuse...");
        if (myClass.getClass().getMethod(method.getName(), method.getParameterTypes()).isAnnotationPresent(Log.class)) {
          loggedMethodsCache.put(method, true);
        } else {
          loggedMethodsCache.put(method, false);
        }
      }
      return loggedMethodsCache.get(method);
    }

    @Override
    public String toString() {
      return "DemoInvocationHandler{" +
          "myClass=" + myClass +
          '}';
    }
  }
}