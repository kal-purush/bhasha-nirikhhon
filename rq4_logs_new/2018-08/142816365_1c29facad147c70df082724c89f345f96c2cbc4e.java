package com.zm.beans.support;

import com.zm.beans.factory.config.SingletonBeanRegistry;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultSingtonBeanRegistry implements SingletonBeanRegistry {
    private final Map<String, Object> singletonObjects = new ConcurrentHashMap<>(64);

    @Override
    public void registrySingleton(String beanName, Object singletonObject) {
        Assert.notNull(beanName, "beanName not null");
        if (this.singletonObjects.get(beanName) != null) {
            throw new IllegalArgumentException("SingletonObject already registry under beanName : " + beanName );
        }
        this.singletonObjects.put(beanName, singletonObject);
    }

    @Override
    public Object getSingleton(String beanName) {
        return this.singletonObjects.get(beanName);
    }
}