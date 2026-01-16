package de.thberger.cinevote.calendar;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class CinemaEventTest {
    @Test
    public void equals() throws Exception {
        WebCalendar webCalendar = mock(WebCalendar.class);
        CinemaEvent e1 = createEvent(webCalendar);
        CinemaEvent e2 = createEvent(webCalendar);
        assertThat(e1).isEqualTo(e2);

        e2.setLocation("Paris");
        assertThat(e1).isNotEqualTo(e2);
    }

    private CinemaEvent createEvent(WebCalendar webCalendar) {
        CinemaEvent event = new CinemaEvent();
        event.setCaption("Testevent");
        event.setVisible(true);
        event.setUrl("http://example.com");
        event.setLocation("Berlin");
        event.setDescription("Test event");
        event.setStart(getDate("2017-12-03T10:15:30.00Z"));
        event.setEnd(getDate("2017-12-03T15:15:30.00Z"));
        event.setAllDay(false);
        event.setStyleName("theme1");
        event.setParent(webCalendar);
        event.setTimeZone(ZoneId.systemDefault());
        return event;
    }

    private Date getDate(String sDate) {
        return Date.from(Instant.parse(sDate));
    }

}