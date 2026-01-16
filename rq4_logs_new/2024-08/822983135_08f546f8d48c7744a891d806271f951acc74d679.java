package org.hhplus.hhplus_concert_service.business.service.listener.concert;

import lombok.extern.slf4j.Slf4j;
import org.hhplus.hhplus_concert_service.domain.OutboxEvent;
import org.hhplus.hhplus_concert_service.persistence.OutBoxEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class OutBoxChangeSeatStatusPublisher {

    private final OutBoxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String CHANGE_SEAT_STATUS_TOPIC = "changeSeatStatus-topic";
    private static final int MAX_RETRIES = 3;

    public OutBoxChangeSeatStatusPublisher(OutBoxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 5000)  // 5초마다 실행
    public void publishOutboxEvents() {
        List<OutboxEvent> events = outboxEventRepository.findByProcessedFalse();

        for (OutboxEvent event : events) {
            boolean sent = false;
            int attempt = 0;

            try {
                // 이벤트를 처리 중으로 표시하여 중복 발행 방지
                event.setInProgress(true);
                outboxEventRepository.save(event);

                while (!sent && attempt < MAX_RETRIES) {
                    try {
                        kafkaTemplate.send(CHANGE_SEAT_STATUS_TOPIC, event.getPayload()).get();

                        // 메시지 전송이 성공하면 아웃박스 이벤트를 처리 완료로 표시
                        event.setProcessed(true);
                        event.setInProgress(false); // 처리 중 상태 해제
                        outboxEventRepository.save(event);
                        outboxEventRepository.save(event);
                        sent = true; // 전송 성공 시 sent 플래그를 true로 설정
                    } catch (Exception e) {
                        attempt++;
                        log.error("Failed to send event to Kafka, attempt {}/{}: {}", attempt, MAX_RETRIES, event, e);

                        if (attempt >= MAX_RETRIES) {
                            log.error("Max retry attempts reached for event: {}", event);

                        }

                        try {
                            // 재시도 전에 잠시 대기
                            Thread.sleep(1000); // 1초 대기 (원하는 대기 시간으로 조절 가능)
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            log.error("Retry sleep interrupted", ie);
                        }
                    }
                }

            } catch (Exception e) {
                log.error("Failed to process event: {}", event, e);
                event.setInProgress(false); // 실패 시 처리 중 상태 해제
                outboxEventRepository.save(event);
            }
        }
    }
}