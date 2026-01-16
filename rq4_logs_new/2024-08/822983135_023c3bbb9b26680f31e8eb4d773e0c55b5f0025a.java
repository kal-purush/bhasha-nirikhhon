package org.hhplus.hhplus_concert_service.business.service.event.reservation;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;
;

@Getter
public class OnReservationCompletedEvent extends ApplicationEvent {
    private final String userId;
    private final int concertId;
    private final int reservationId;
    private final int paymentId;

    public OnReservationCompletedEvent(Object source, String userId, int concertId, int reservationId, int paymentId) {
        super(source);
        this.userId = userId;
        this.concertId = concertId;
        this.reservationId = reservationId;
        this.paymentId = paymentId;
    }

    public String getUserId() {
        return userId;
    }

    public int getConcertId() {
        return concertId;
    }

    public int getReservationId() {
        return reservationId;
    }

    public int getPaymentId() {
        return paymentId;
    }
}