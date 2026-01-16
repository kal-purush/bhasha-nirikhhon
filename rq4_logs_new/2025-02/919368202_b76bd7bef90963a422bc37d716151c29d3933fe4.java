package io.quarkiverse.hibernate.cli;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ConfigUtil {

    public static void doSomething() {
        ClassLoader quarkusClassLoader = Thread.currentThread().getContextClassLoader();
        System.out.println("Quarkus augment class loader : " + quarkusClassLoader.getClass().getName());
        Class<?> smallRyeConfigProviderResolverClass = lookup("io.smallrye.config.SmallRyeConfigProviderResolver",
                quarkusClassLoader);
        System.out.println(
                "SmallRyeConfigProviderResolverClass: " + smallRyeConfigProviderResolverClass.getName().getClass());
        Object smallRyeProviderResolver = construct(smallRyeConfigProviderResolverClass);
        System.out.println("smallRyeProviderResolver is created: " + smallRyeProviderResolver.getClass().getName());
        Object config = runMethod(smallRyeProviderResolver, "getConfig", quarkusClassLoader);
        System.out.println("config is created : " + config);
        Object configValue = runGetConfigValueMethod(config, "quarkus.datasource.jdbc.url");
        System.out.println("JDBC URL Config Value: " + configValue);
        Object value = runGetValueMethod(configValue);
        System.out.println("  JDBC URL: " + runGetValueMethod(configValue));
    }

    private static Class<?> lookup(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static Object runMethod(Object object, String methodName, ClassLoader loader) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, new Class[] { ClassLoader.class });
            System.out.println("Method found: " + method);
            return object.getClass().getDeclaredMethod(methodName, new Class[] { ClassLoader.class }).invoke(object, loader);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object construct(Class<?> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object runGetConfigValueMethod(Object receiver, String configName) {
        Object result = null;
        try {
            Method m = receiver.getClass().getDeclaredMethod("getConfigValue", new Class[] { String.class });
            System.out.println("getConfigValue method is found: " + m);
            result = m.invoke(receiver, configName);
            System.out.println("returning result: " + result);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private static Object runGetValueMethod(Object receiver) {
        Object result = null;
        try {
            Method m = receiver.getClass().getDeclaredMethod("getValue");
            System.out.println("getValue method was found: " + m);
            result = m.invoke(receiver);
            System.out.println("returning result: " + result);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}