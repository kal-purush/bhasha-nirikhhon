package com.joizhang.naiverpc.spring.context;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;

@Getter
@Setter
public class NaiveRpcSpringInitContext {

    private BeanDefinitionRegistry registry;

    private ConfigurableListableBeanFactory beanFactory;

    private ApplicationContext applicationContext;

    private volatile boolean bound;

    public void markAsBound() {
        bound = true;
    }

}