package de.thberger.cinevote.ui;

import com.vaadin.spring.annotation.SpringComponent;
import com.vaadin.ui.Calendar;
import com.vaadin.ui.components.calendar.CalendarComponentEvents;
import de.thberger.cinevote.AppConfig;
import de.thberger.cinevote.calendar.CinemaEvent;
import de.thberger.cinevote.calendar.WebCalendarEventProvider;
import lombok.AllArgsConstructor;

import javax.annotation.PostConstruct;

/**
 * @author thb
 */
@SpringComponent
@AllArgsConstructor
public class CineCalendar extends Calendar {

    private WebCalendarEventProvider calendarEventProvider;

    private MovieDetailsPanel detailsPanel;

    private AppConfig appConfig;

    @PostConstruct
    public void init() {
        setEventProvider(calendarEventProvider);
        setCaptionAsHtml(true);
        setWidth("100%");
        setHeight("100%");
        setFirstVisibleHourOfDay(appConfig.getDayView().getFirstVisibleHourOfDay());
        setLastVisibleHourOfDay(appConfig.getDayView().getLastVisibleHourOfDay());
        setHandler(eventClickHandler());
        setHandler((CalendarComponentEvents.DateClickHandler) event -> {
        });
    }

    void switchView(View view) {
        setStartDate(view.getStartDate());
        setEndDate(view.getEndDate());
    }

    private CalendarComponentEvents.EventClickHandler eventClickHandler() {
        return (CalendarComponentEvents.EventClickHandler) event ->
                detailsPanel.update((CinemaEvent) event.getCalendarEvent());
    }
}