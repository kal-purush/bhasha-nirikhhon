package org.hhplus.hhplus_concert_service.business.service.event.point;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class PointMinusEvent extends ApplicationEvent {
    private final String userId;
    private final int totalPrice;
    private final int concertId;

    public PointMinusEvent(Object source, String userId, int totalPrice, int concertId) {
        super(source);
        this.userId = userId;
        this.totalPrice = totalPrice;
        this.concertId = concertId;
    }

    public String getUserId() {
        return userId;
    }

    public int getTotalPrice() {
        return totalPrice;
    }

    public int getConcertId() {
        return concertId;
    }

}