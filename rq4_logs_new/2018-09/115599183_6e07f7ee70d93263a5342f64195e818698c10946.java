package com.chelsea.java_common;

import java.io.InputStream;

/**
 * 自定义类加载器
 * 
 * @author shevchenko
 *
 */
public class MyClassLoader extends ClassLoader {

    @Override
    protected Class<?> findClass(String path) throws ClassNotFoundException {
        try {
            InputStream is = this.getClass().getResourceAsStream(path);
            byte[] h = new byte[is.available()];
            is.read(h);
            String className = path.replaceAll("/", ".").substring(1, path.lastIndexOf("."));
            return defineClass(className, h, 0, h.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        String path = "/com/chelsea/java_common/HelloService.class";
        while (true) {
            MyClassLoader loder = new MyClassLoader();
            // 通过自定义类加载器加载类
            Class<?> claee = loder.findClass(path);
            // 得到对象的实例
            Object object = claee.newInstance();
            IHelloService helloService = (IHelloService) claee.cast(object);
            helloService.sayHello();
            System.gc();
            Thread.currentThread().sleep(5000);
        }
    }

}