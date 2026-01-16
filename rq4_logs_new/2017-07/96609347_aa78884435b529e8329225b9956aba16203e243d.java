package de.thberger.cinevote.ui;

import com.vaadin.annotations.Theme;
import com.vaadin.server.VaadinRequest;
import com.vaadin.spring.annotation.SpringUI;
import com.vaadin.ui.*;
import com.vaadin.ui.components.calendar.CalendarComponentEvents;
import de.thberger.cinevote.calendar.CinemaEvent;
import de.thberger.cinevote.calendar.WebCalendarEventProvider;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Locale;

import static de.thberger.cinevote.helpers.DateHelpers.getDayInFourWeeks;
import static de.thberger.cinevote.helpers.DateHelpers.getFirstDayOfWeek;


@SuppressWarnings("serial")
@Theme("vaadincalendar")
@SpringUI
public class CineVoteUI extends UI {


    private Calendar calendar;
    private WebCalendarEventProvider calendarEventProvider;
    private Label caption;
    private MovieDetailsPanel detailPanel;

    @Autowired
    public void setCalendarEventProvider(WebCalendarEventProvider eventProvider) {
        calendarEventProvider = eventProvider;
    }

    @Override
    protected void init(VaadinRequest request) {
        this.setLocale(Locale.GERMANY);
        setupUiComponents();
        showMonth();
    }

    private void setupUiComponents() {
        final VerticalLayout vLayout = new VerticalLayout();
        vLayout.setMargin(true);
        setContent(vLayout);

        HorizontalLayout topBar = createTopBar();
        vLayout.addComponent(topBar);
        vLayout.addComponent(createCalendarBar());

    }

    private HorizontalLayout createCalendarBar() {
        HorizontalLayout h = new HorizontalLayout();
        h.setSizeFull();
        h.setSpacing(true);
        calendar = createCalendarWidget();
        detailPanel = new MovieDetailsPanel();
        h.addComponent(calendar);
        h.addComponent(detailPanel);
        h.setExpandRatio(calendar, 4);
        h.setExpandRatio(detailPanel, 1);
        return h;
    }

    private HorizontalLayout createTopBar() {
        HorizontalLayout topBar = new HorizontalLayout();
        topBar.setStyleName("topBar");
        topBar.setSpacing(true);
        caption = new Label();
        caption.setStyleName("main-title");
        topBar.addComponent(caption);
        return topBar;
    }

    private void showMonth() {
        setTitle("Monatsansicht");
        calendar.setStartDate(getFirstDayOfWeek());
        calendar.setEndDate(getDayInFourWeeks());
    }

    private Calendar createCalendarWidget() {
        Calendar calendar = new Calendar();
        calendar.setEventProvider(calendarEventProvider);
        calendar.setCaptionAsHtml(true);
        calendar.setWidth("100%");
        calendar.setHeight("100%");
        calendar.setFirstVisibleHourOfDay(19);
        calendar.setLastVisibleHourOfDay(24);
        calendar.setHandler(eventClickHander());
        calendar.setHandler((CalendarComponentEvents.DateClickHandler) event -> {
        });
        return calendar;
    }

    private CalendarComponentEvents.EventClickHandler eventClickHander() {
        return (CalendarComponentEvents.EventClickHandler) event
                -> detailPanel.update((CinemaEvent) event.getCalendarEvent());
    }

    private void setTitle(final String subTitle) {
        String title = "Programm Freiluftkinos - " + subTitle;
        caption.setValue(title);
        super.getPage().setTitle(title);
    }

}