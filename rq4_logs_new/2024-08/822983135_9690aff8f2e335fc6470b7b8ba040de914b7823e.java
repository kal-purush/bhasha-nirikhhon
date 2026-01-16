package org.hhplus.hhplus_concert_service.business.service.event.concert;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class ConcertInsertEvent extends ApplicationEvent {
    private final String status;
    private final String title;


    public ConcertInsertEvent(Object source, String status, String title) {
        super(source);
        this.status = status;
        this.title = title;
    }

    public String getStatus() {
        return status;
    }

    public String getTitle() {
        return title;
    }
}