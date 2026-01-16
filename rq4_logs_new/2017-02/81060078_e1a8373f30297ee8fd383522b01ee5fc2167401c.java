package com.thetechiehouse.messageapp;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.thetechiehouse.messageapp.messageapp.rabbitmq.config.CustomMessage;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            System.out.println("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                //System.out.println(beanName);
            }

            
            
 RabbitTemplate rabbitTemplate = ctx.getBean(RabbitTemplate.class);

 AtomicInteger counter = new AtomicInteger();
 for (int i = 0; i < 5; i++){
     System.out.println("sending new custom message..");
     rabbitTemplate.convertAndSend(new CustomMessage(counter.incrementAndGet(), "RabbitMQ Spring JSON Example"));
 }

        };
    }

}