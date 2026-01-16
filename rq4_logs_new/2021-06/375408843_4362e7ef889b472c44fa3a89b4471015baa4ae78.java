package com.pinest94.javatest;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;

public class FindSlowTestExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

    private long THRESHOLD;

    FindSlowTestExtension(long threshold) {
        this.THRESHOLD = threshold;
    }

    @Override
    public void beforeTestExecution(ExtensionContext extensionContext) throws Exception {
        ExtensionContext.Store store = getStore(extensionContext);
        store.put("START_TIME", System.currentTimeMillis());
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) throws Exception {

        Method requiredMethodTest = extensionContext.getRequiredTestMethod();
        SlowTest annotation = requiredMethodTest.getAnnotation(SlowTest.class);

        ExtensionContext.Store store = getStore(extensionContext);
        String testMethodName = extensionContext.getRequiredTestMethod().getName();


        long startTime = store.remove("START_TIME", long.class);
        long duration = System.currentTimeMillis() - startTime;

        if(duration > THRESHOLD && annotation == null) {
            System.out.printf("Please consider mark method [%s] with @SlowTest. \n", testMethodName);
        }
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        String testClassName = context.getRequiredTestClass().getName();
        String testMethodName = context.getRequiredTestMethod().getName();

        return context.getStore(ExtensionContext.Namespace.create(
                testClassName, testMethodName
        ));
    }
}