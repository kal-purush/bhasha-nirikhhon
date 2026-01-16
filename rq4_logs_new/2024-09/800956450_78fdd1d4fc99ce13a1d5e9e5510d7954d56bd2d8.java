package io.github.xpakx.images_cdc.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

@Profile("rabbit")
@Configuration
public class AMQPConfig {
    @Bean
    public TopicExchange eventTopicExchange(
            @Value("${amqp.exchange.events}") final String eventTopic) {
        return ExchangeBuilder
                .topicExchange(eventTopic)
                .durable(true)
                .build();
    }

    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public Queue accountQueue(@Value("${amqp.queue.account}") final String queueName) {
        return QueueBuilder.durable(queueName).build();
    }

    @Bean
    public Binding accountBinding(final Queue accountQueue, final TopicExchange eventTopicExchange) {
        return BindingBuilder.bind(accountQueue)
                .to(eventTopicExchange)
                .with("account");
    }

    @Bean
    public Queue imagesQueue(@Value("${amqp.queue.image}") final String queueName) {
        return QueueBuilder.durable(queueName).build();
    }

    @Bean
    public Binding imageBinding(final Queue imagesQueue, final TopicExchange eventTopicExchange) {
        return BindingBuilder.bind(imagesQueue)
                .to(eventTopicExchange)
                .with("image");
    }

    @Bean
    public MessageHandlerMethodFactory messageHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        final MappingJackson2MessageConverter jsonConverter = new MappingJackson2MessageConverter();
        jsonConverter.getObjectMapper().registerModule(
                new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));
        factory.setMessageConverter(jsonConverter);
        return factory;
    }

    @Bean
    public RabbitListenerConfigurer rabbitListenerConfigurer(
            final MessageHandlerMethodFactory messageHandlerMethodFactory) {
        return (c) -> c.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
    }
}