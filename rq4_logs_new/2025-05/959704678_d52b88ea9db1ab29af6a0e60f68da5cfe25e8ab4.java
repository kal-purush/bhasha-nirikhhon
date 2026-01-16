package com.example.docconneting.domain.coupon.config;

import com.example.docconneting.common.exception.object.ClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ErrorHandler;

@Slf4j
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    @Value("${rabbitmq.queue.dlq-name}")
    private String dlqQueueName;

    @Value("${rabbitmq.queue.retry-name}")
    private String retryQueueName;

    @Value("${rabbitmq.queue.fail-name}")
    private String failQueueName;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.routing.key}")
    private String routingKey;

    @Value("${rabbitmq.routing.dlq-key}")
    private String dlqRoutingKey;

    @Value("${rabbitmq.routing.retry-key}")
    private String retryRoutingKey;

    @Value("${rabbitmq.routing.fail-key}")
    private String failRoutingKey;

    // 기본 큐
    @Bean
    public Queue queue() {
        return QueueBuilder.durable(queueName)
                .withArgument("x-dead-letter-exchange", exchangeName)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    // DLQ (죽은 메세지 큐)
    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(dlqQueueName).build();
    }

    // 재시도 큐
    @Bean
    public Queue retryQueue() {
        return QueueBuilder.durable(retryQueueName).build();
    }

    // 최종 실패 정보 저장 큐
    @Bean
    public Queue failQueue() {
        return QueueBuilder.durable(failQueueName).build();
    }

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(exchangeName);
    }

    // 기본 큐에 바인딩
    @Bean
    public Binding binding(Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(routingKey);
    }

    // DLQ 바인딩
    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(dlqQueue()).to(exchange()).with(dlqRoutingKey);
    }

    // 재시도 큐에 바인딩
    @Bean
    public Binding retryBinding() {
        return BindingBuilder.bind(retryQueue()).to(exchange()).with(retryRoutingKey);
    }

    // 실패 큐 바인딩
    @Bean
    public Binding failBinding() {
        return BindingBuilder.bind(failQueue()).to(exchange()).with(failRoutingKey);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setErrorHandler(customErrorHandler());
        return factory;
    }

    // 비즈니스 예외는 재시도 없이 버리는 에러 핸들러
    @Bean
    public ErrorHandler customErrorHandler() {
        return new ConditionalRejectingErrorHandler(new ConditionalRejectingErrorHandler.DefaultExceptionStrategy() {
            @Override
            public boolean isFatal(Throwable t) {
                Throwable cause = t.getCause();
                if (cause instanceof ClientException) {
                    log.warn("비즈니스 예외 발생. 메시지 버림: {}", cause.getMessage());
                    return false; // ClientException는 → ACK 처리됨
                }
                return true; // 그 외는 치명적 → 재시도 또는 DLQ
            }
        });
    }
}