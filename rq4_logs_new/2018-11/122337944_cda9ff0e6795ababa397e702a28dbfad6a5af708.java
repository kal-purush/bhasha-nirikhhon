package me.snowdrop.fallback.interceptor;

import me.snowdrop.fallback.ExecutionContext;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;

/**
 * Handled the actual of invocation of the original method and if an throwable is thrown by it,
 * is it handled by the proper fallback method.
 * The proper fallback method is determined by the type of throwable that was thrown
 */
public class FallbackInterceptor implements MethodInterceptor {

    private final List<Configuration> configurationList;

    public FallbackInterceptor(List<Configuration> configurationList) {
        Collections.sort(configurationList);
        this.configurationList = configurationList;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        try {
            return invocation.proceed();
        } catch (Throwable throwable) {
            for (Configuration conf : configurationList) {
                if (conf.getExceptionToHandle().isAssignableFrom(throwable.getClass())) {
                    return invokeFallback(invocation, throwable, conf.getHandlerObject(), conf.getTargetFallbackMethod());
                }
            }
            //none of the fallback handlers can handle the throwable so just rethrow it
            throw throwable;
        }
    }

    private Object invokeFallback(MethodInvocation invocation, Throwable throwable,
                                  Object targetObject, Method targetMethod)
            throws InvocationTargetException, IllegalAccessException {

        // ensure that we can invoke private methods as fallbacks
        if (Modifier.isPrivate(targetMethod.getModifiers())) {
            targetMethod.setAccessible(true);
        }

        final Class<?>[] parameterTypes = targetMethod.getParameterTypes();
        if (parameterTypes.length == 0) { //target method takes no parameters
            return targetMethod.invoke(targetObject);
        }

        if ((parameterTypes.length == 1) && (ExecutionContext.class.equals(parameterTypes[0]))) {
            return targetMethod.invoke(targetObject, DefaultExecutionContext.fromMethodInvocation(invocation, throwable));
        }

        throw new IllegalArgumentException("The target method " + targetMethod + " is not valid" +
                "Either use no parameters to the fallback handler or a single parameter of type:" + ExecutionContext.class);
    }

    public static class Configuration implements Comparable<Configuration>{

        private final Method targetFallbackMethod;
        private final Object handlerObject;
        private final Class<? extends Throwable> exceptionToHandle;
        // the smaller the number, the higher the order of this configuration
        private final int order;

        public Configuration(Method targetFallbackMethod,
                             Object handlerObject,
                             Class<? extends Throwable> exceptionToHandle,
                             int order) {
            this.targetFallbackMethod = targetFallbackMethod;
            this.handlerObject = handlerObject;
            this.exceptionToHandle = exceptionToHandle;
            this.order = order;
        }

        public Method getTargetFallbackMethod() {
            return targetFallbackMethod;
        }

        public Object getHandlerObject() {
            return handlerObject;
        }

        public Class<? extends Throwable> getExceptionToHandle() {
            return exceptionToHandle;
        }

        public int getOrder() {
            return order;
        }

        @Override
        public int compareTo(Configuration o) {
            return getOrder() - o.getOrder();
        }
    }
}