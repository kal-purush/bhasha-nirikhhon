package com.lym.business.simple.ack;

import com.lym.business.simple.config.RabbitMqConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Consumer {
    public static void main(String[] args) throws Exception{

        ConnectionFactory factory = RabbitMqConfig.connectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        String exchangeName ="test_ack_exchange";
        String routingKey = "ack.save";
        String queueName = "test_ack_queue";

        channel.exchangeDeclare(exchangeName,"topic",false,false,null);
        channel.queueDeclare(queueName,false,false,false,null);
        channel.queueBind(queueName,exchangeName,routingKey);

        channel.basicConsume(queueName,false,new MyConsumer(channel));


    }
}