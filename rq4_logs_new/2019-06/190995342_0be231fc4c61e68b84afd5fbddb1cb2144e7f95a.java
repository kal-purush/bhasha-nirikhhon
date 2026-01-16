package com.github.ssproessig.amqptraining.springbootconsumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
public class RabbitConfiguration {

  @Value("${consumer-sample.queue-name:spring-boot-consumer}")
  private String queueName;

  @Value("${consumer-sample.binding-key:*.info}")
  private String bindingKey;

}