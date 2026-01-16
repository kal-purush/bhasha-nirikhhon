package org.hhplus.hhplus_concert_service.business.service.listener.tokenQueue;

import lombok.extern.slf4j.Slf4j;
import org.hhplus.hhplus_concert_service.domain.OutboxEvent;
import org.hhplus.hhplus_concert_service.persistence.OutBoxEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Slf4j
@Service
public class OutBoxDeleteTokenEventPublisher {

    @Autowired
    private OutBoxEventRepository outboxEventRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String DELETE_TOKEN_TOPIC = "deleteToken-topic";
    private static final int MAX_RETRIES = 3;

    @Scheduled(fixedRate = 5000) // 5초마다 실행
    public void publishOutboxEvents() {
        List<OutboxEvent> events = outboxEventRepository.findAll();

        for (OutboxEvent event : events) {
            if ("DELETE_TOKEN".equals(event.getEventType())) {
                sendMessageWithRetry(event, DELETE_TOKEN_TOPIC, 0);
            }
        }
    }

    private void sendMessageWithRetry(OutboxEvent event, String topic, int attempt) {
        ListenableFuture<SendResult<String, String>> future = (ListenableFuture<SendResult<String, String>>) kafkaTemplate.send(topic, event.getPayload());

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                // 메시지 전송이 성공하면 아웃박스에서 이벤트 삭제
                outboxEventRepository.delete(event);
            }

            @Override
            public void onFailure(Throwable ex) {
                // 실패 시 재시도 또는 실패 처리
                if (attempt < MAX_RETRIES) {
                    sendMessageWithRetry(event, topic, attempt + 1);
                } else {
                    // 재시도 한계를 초과하면 로그를 남기고, 필요하면 장애 처리 로직 추가
                    log.error("Failed to send message after {} attempts, event: {}", MAX_RETRIES, event, ex);
                    handleFailedEvent(event);
                }
            }
        });
    }

    private void handleFailedEvent(OutboxEvent event) {

        log.error("Failed to process event: {}", event);
        // 필요 시 추가적인 처리 로직 구현
    }
}