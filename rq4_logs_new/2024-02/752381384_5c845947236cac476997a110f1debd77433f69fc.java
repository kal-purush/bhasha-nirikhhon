package com.example.kafkacustom;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class KafkaListenerCustomInitializer {

    private final MessageBroker messageBroker;
    private final ApplicationContext context;

    private final Map<String, Map<Object, Method>> registeredListeners = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        discoverListeners();
    }

    private void discoverListeners() {
        String[] beanNames = context.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Object bean = context.getBean(beanName);
            for (Method method : bean.getClass().getDeclaredMethods()) {
                if (method.isAnnotationPresent(KafkaListenerCustom.class)) {
                    KafkaListenerCustom annotation = method.getAnnotation(KafkaListenerCustom.class);
                    registeredListeners.computeIfAbsent(annotation.topicName(), k -> new ConcurrentHashMap<>()).put(bean, method);
                }
            }
        }
    }

    @Scheduled(fixedRate = 1000)
    public void pollMessages() {
        registeredListeners.forEach((topicName, listeners) -> {
            List<String> messages = messageBroker.consumeMessage(topicName);
            if (!messages.isEmpty()) {
                listeners.forEach((bean, method) -> invokeListenerMethod(bean, method, messages));
            }
        });
    }

    private void invokeListenerMethod(Object bean, Method method, List<String> messages) {
        for (String message : messages) {
            try {
                method.setAccessible(true);
                method.invoke(bean, message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}