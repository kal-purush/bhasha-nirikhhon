package org.example.bean;

import org.example.annotation.RandomValue;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Random;

@Component
public class RandomValueBeanPostProcessor implements BeanPostProcessor {
    private final Random random = new Random();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Field[] fields = bean.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(RandomValue.class)) {
                RandomValue annotation = field.getAnnotation(RandomValue.class);
                double min = annotation.min();
                double max = annotation.max();
                double randomValue = min + (max - min) * random.nextDouble();
                field.setAccessible(true);
                try {
                    field.set(bean, randomValue);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return bean;
    }
}