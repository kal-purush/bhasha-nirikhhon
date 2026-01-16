package org.hhplus.hhplus_concert_service.business.service.event.payment;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class PaymentEvent extends ApplicationEvent {
    private final int paymentAmount;
    private final int reservationId;


    public PaymentEvent(Object source, int paymentAmount, int reservationId) {
        super(source);
        this.paymentAmount = paymentAmount;
        this.reservationId = reservationId;
    }

    public int getPaymentAmount() {
        return paymentAmount;
    }

    public int getReservationId() {
        return reservationId;
    }
}