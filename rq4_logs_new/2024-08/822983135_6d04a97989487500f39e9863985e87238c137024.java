package org.hhplus.hhplus_concert_service.outboxTest;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hhplus.hhplus_concert_service.business.service.ReservationService;
import org.hhplus.hhplus_concert_service.domain.OutboxEvent;
import org.hhplus.hhplus_concert_service.persistence.OutBoxEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"change-seat-status-topic"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OutBoxTest {

    @Autowired
    private OutBoxEventRepository outboxEventRepository;

    @Autowired
    private ReservationService reservationService;  // 예약 서비스


    private BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "change-seat-status-topic")
    public void listen(ConsumerRecord<String, String> record) {
        records.add(record);
    }

    @Test
    @Transactional
    @DisplayName("아웃박스 저장 테스트")
    public void testOutboxEventStorage() {
        // Given
        String userId = "user123";
        int concertId = 1;
        int itemId = 2;
        int seatId = 3;
        int totalPrice = 10000;
        String status = "T";

        // When
        reservationService.reservation(userId, concertId, itemId, seatId, totalPrice, status);

        // Then
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).isNotEmpty();
        OutboxEvent event = outboxEvents.get(0);
        assertThat(event.getEventType()).isEqualTo("CHANGE_SEAT_STATUS");
        assertThat(event.getPayload()).contains("\"seatId\":" + seatId);
    }

}