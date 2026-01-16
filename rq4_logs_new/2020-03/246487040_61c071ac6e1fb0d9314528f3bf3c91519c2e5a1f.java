package org.mycode.annotationbased;

import org.mycode.model.ActivatorOfReactor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class AppReactorAnnotationRunner {
    public static void main(String[] args) {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SpringAopConfig.class);
        ActivatorOfReactor activator = applicationContext.getBean(ActivatorOfReactor.class);
        activator.runReactor();
        activator.stopReactor();
    }
}