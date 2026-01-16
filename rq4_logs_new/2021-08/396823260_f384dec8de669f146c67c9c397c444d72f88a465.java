package my_spring.object_factory.annotation.proxy.creator;

import my_spring.object_factory.annotation.proxy.Benchmark;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProxyCreator {
    public <T> T createProxy(Class<T> type, T obj, Class<? extends Annotation> proxyAnnotation, ProxyHandler proxyHandler) {
        Set<String> methodWithBenchmarkAnnotationNames = new HashSet<>();
        fillMethodNames(type, proxyAnnotation, methodWithBenchmarkAnnotationNames);

        if (!methodWithBenchmarkAnnotationNames.isEmpty()) {
            if (type.getInterfaces().length != 0) {
                return createProxyByInterface(type, obj, methodWithBenchmarkAnnotationNames, proxyHandler);
            }

            return createProxyByClass(type, obj, methodWithBenchmarkAnnotationNames, proxyHandler);
        }

        return obj;
    }

    private static <T> void fillMethodNames(Class<T> type, Class<? extends Annotation> proxyAnnotation, Set<String> methodWithBenchmarkAnnotationNames) {
        if (type.isAnnotationPresent(proxyAnnotation)) {
            List<String> methodNames = Arrays.stream(type.getDeclaredMethods())
                    .map(Method::getName).collect(Collectors.toList());
            methodWithBenchmarkAnnotationNames.addAll(methodNames);
        } else {
            for (Method method : type.getDeclaredMethods()) {
                if (method.isAnnotationPresent(proxyAnnotation)) {
                    methodWithBenchmarkAnnotationNames.add(method.getName());
                }
            }
        }
    }

    private <T> T createProxyByInterface(Class<T> type, T obj, Set<String> methodNames, ProxyHandler proxyHandler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                type.getInterfaces(),
                (proxy, method, args) -> {
                    if (methodNames.contains(method.getName())) {
                        return proxyHandler.handle(obj, method, args);
                    }

                    return method.invoke(obj, args);
                }
        );
    }

    private <T> T createProxyByClass(Class<T> type, T obj, Set<String> methodNames, ProxyHandler proxyHandler) {
        return (T) Enhancer.create(type,
                (MethodInterceptor) (o, method, args, proxy) -> {
                    if (methodNames.contains(method.getName())) {
                        return proxyHandler.handle(obj, method, args);
                    }
                    return method.invoke(obj, args);
        });
    }
}