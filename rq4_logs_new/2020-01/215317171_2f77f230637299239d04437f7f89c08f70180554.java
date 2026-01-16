package fr.esir.jxc.services;

import fr.esir.jxc.config.KafkaProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import org.springframework.util.concurrent.SettableListenableFuture;

@Service
public class KafkaProducer {
    @Autowired
    private final KafkaProducerConfig kafkaProducerConfig;
    @Autowired
    private final KafkaTemplate<String, Event> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Event> kafkaTemplate, KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public ListenableFuture<SendResult<String, Event>> produce(Event event) {
        return this.kafkaTemplate.send(this.kafkaProducerConfig.TOPIC, event);
    }

    public ListenableFuture<SendResult<String, Event>> produceObject(Object event) {
        return Event.of(event)
                .map(this::produce)
                .orElseGet(() -> {
                    SettableListenableFuture<SendResult<String, Event>> error = new SettableListenableFuture<>();
                    error.setException(new RuntimeException());
                    error.cancel(true);
                    return error;
                });
    }

}