package com.techchallenge.infrastructure.message.consumer;


import com.techchallenge.application.usecases.OrderUseCase;
import com.techchallenge.infrastructure.message.consumer.dto.MessagePaymentDto;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CountDownLatch;

@Log4j2
public class PaymentConsumer {

    private final OrderUseCase orderUseCase;
    //private final NotifyCustomerUseCase notifyCustomerUseCase;

    private CountDownLatch latch = new CountDownLatch(1);


    public PaymentConsumer(OrderUseCase orderUseCase) {
        this.orderUseCase = orderUseCase;
    }

    @KafkaListener(topics = "${kafka.topic.consumer.error.payment}",
            groupId = "${kafka.topic.consumer.error.groupId}",
            containerFactory = "kafkaListenerContainerFactoryMessagePaymentDto")
    public void listenPayment(MessagePaymentDto message, Acknowledgment ack) {
        log.info("Received Message Error Payment: {}", message);
        try {
            orderUseCase.cancelOrder(message.externalReference());
            ack.acknowledge();
            latch.countDown();
        }catch (Exception ex){
            log.error("Message not process: "+ ex.getMessage());
        }
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}