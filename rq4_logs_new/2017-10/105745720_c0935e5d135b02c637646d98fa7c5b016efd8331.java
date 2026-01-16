package com.allianz.training.cloud;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Argument;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

//@RabbitListener(queues = "com.allianz.first")

@Component
public class RabbitListener1 {

    @Value("${server.port}")
    private int port;

    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = "test.response",
                                                            autoDelete = "true",
                                                            arguments = @Argument(name = "x-message-ttl",
                                                                                  value = "10000",
                                                                                  type = "java.lang.Integer"),
                                                            durable = "true"),
                                             exchange = @Exchange(value = "respexchange",
                                                                  type = ExchangeTypes.DIRECT,
                                                                  autoDelete = "false",
                                                                  durable = "true"),
                                             key = "consumer"))
    public void handleMessage(final String msg) {
        System.out.println("Response message : " + msg + " port : " + this.port);
        // return "H:" + headerTest + " M:" + msg;
    }

}