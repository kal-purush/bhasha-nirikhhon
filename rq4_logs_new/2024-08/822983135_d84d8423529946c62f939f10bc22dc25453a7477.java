package org.hhplus.hhplus_concert_service.business.service.listener.concert;

import lombok.RequiredArgsConstructor;
import org.hhplus.hhplus_concert_service.business.service.event.concert.OnChangeSeatStatusEvent;
import org.hhplus.hhplus_concert_service.business.service.listener.reservation.OnReservationCompletedListener;
import org.hhplus.hhplus_concert_service.domain.ConcertSeat;
import org.hhplus.hhplus_concert_service.persistence.ConcertSeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OnChangeSeatStatusListener {

    private final ConcertSeatRepository concertSeatRepository;
    private static final Logger log = LoggerFactory.getLogger(OnChangeSeatStatusListener.class);

    @KafkaListener(topics = "seat_topic", groupId = "seat-group")
    public void handleSeatChange(OnChangeSeatStatusEvent event) {
        if(event == null) {
            log.error("Received null event");
            throw new IllegalArgumentException("SeatEvent cannot be null");
        }

        try {
            int seatId = event.getSeatId();

            ConcertSeat concertSeat = concertSeatRepository.findById(seatId).orElse(null);

            concertSeat.setStatus("Y");
            concertSeatRepository.save(concertSeat);

        } catch (RuntimeException e) {
            log.error("Failed to handle SeatEvent", e);
            throw new RuntimeException("SeatEvent processing failed", e);
        }
    }
}