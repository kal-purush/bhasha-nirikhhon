package org.example.inmemorybroker;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ListenerBeanPostProcessor implements BeanPostProcessor {

    private final KafkaListenerCustomInitializer kafkaListenerCustomInitializer;

    public ListenerBeanPostProcessor(KafkaListenerCustomInitializer kafkaListenerCustomInitializer) {
        this.kafkaListenerCustomInitializer = kafkaListenerCustomInitializer;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        Arrays.stream(bean.getClass().getMethods())
                .filter(method -> method.isAnnotationPresent(KafkaListenerCustom.class))
                .forEach(method -> {
                    KafkaListenerCustom annotation = method.getAnnotation(KafkaListenerCustom.class);
                    kafkaListenerCustomInitializer.registerListener(annotation.topicName(), bean, method);
                });
        return bean;
    }
}
