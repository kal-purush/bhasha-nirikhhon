package by.dutov.jee;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

public class MyAppContext {
    private static volatile MyAppContext instance;
    private final AbstractApplicationContext ctx;

    private MyAppContext() {
        ctx = new AnnotationConfigApplicationContext("by\\dutov\\jee");
        ctx.registerShutdownHook();
        //singleton
    }

    public static MyAppContext getInstance() {
        if (instance == null) {
            synchronized (MyAppContext.class) {
                if (instance == null) {
                    instance = new MyAppContext();
                }
            }
        }
        return instance;
    }

    public static AbstractApplicationContext getContext() {
        return getInstance().ctx;
    }
}