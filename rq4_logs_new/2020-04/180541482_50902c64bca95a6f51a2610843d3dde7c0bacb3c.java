package com.example.RabbitMq;

import com.example.RabbitMq.message.SimpleMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RabbitMqApplication implements CommandLineRunner {


    @Autowired
    private RabbitTemplate rabbitTemplate;


    public static void main(String[] args) {
        SpringApplication.run(RabbitMqApplication.class, args);
    }

    @Override
    public void run(String... args) {
        SimpleMessage simpleMessage=new SimpleMessage();
        simpleMessage.setName("firstMessage");
        simpleMessage.setDescription("description");

        rabbitTemplate.convertAndSend("TestExchange", "testRouting", simpleMessage);
    }
}