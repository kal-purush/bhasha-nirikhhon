package de.thberger.cinevote;

import com.vaadin.annotations.Theme;
import com.vaadin.server.ExternalResource;
import com.vaadin.server.VaadinRequest;
import com.vaadin.spring.annotation.SpringUI;
import com.vaadin.ui.*;
import com.vaadin.ui.components.calendar.CalendarComponentEvents;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import static de.thberger.cinevote.DateHelpers.getFirstDayOfWeek;
import static de.thberger.cinevote.DateHelpers.getLastDayOfWeek;
import static de.thberger.cinevote.DateHelpers.getNowPlusDays;
import static java.util.Calendar.MONDAY;


@SuppressWarnings("serial")
@Theme("vaadincalendar")
@SpringUI
public class CineVoteUI extends UI {

    private final SimpleDateFormat sf = new SimpleDateFormat("EEE, d. MMMMM HH:mm");
    private Kinokalender kinokalender = new Kinokalender();
    private Calendar calendar;
    private WebCalendarEventProvider calendarEventProvider;
    private Label caption;
    private TextField movieTitle;
    private TextField movieLocation;
    private TextArea movieDescription;
    private TextField movieStart;
    private TextField movieEnd;
    private Link movieUrl;

    @Override
    protected void init(VaadinRequest request) {
        this.setLocale(Locale.GERMANY);
        calendarEventProvider = new WebCalendarEventProvider();
        setupUiComponents();
        updateCalendars();
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
        h.addComponent(calendar);
        Panel detailView = new Panel();
        detailView.setCaption("Filminfos");
        detailView.setStyleName("detailPanel");
        detailView.setContent(createDetailPanel());
        detailView.setSizeFull();
        h.addComponent(detailView);
        h.setExpandRatio(calendar, 4);
        h.setExpandRatio(detailView, 1);
        return h;
    }

    private Layout createDetailPanel() {
        VerticalLayout v = new VerticalLayout();
        v.setSpacing(true);
        v.setSizeFull();
        v.setMargin(true);
        v.setWidth("100%");
        movieTitle = new TextField("Titel");
        movieTitle.setReadOnly(true);
        movieLocation = new TextField("Kino");
        movieLocation.setReadOnly(true);
        movieStart = new TextField("Beginn");
        movieStart.setReadOnly(true);
        movieEnd = new TextField("Ende");
        movieEnd.setReadOnly(true);
        movieDescription = new TextArea("Beschreibung");
        movieDescription.setRows(10);
        movieDescription.setReadOnly(true);
        movieUrl = new Link();
        movieUrl.setStyleName("movieUrl");
        v.addComponents(movieTitle, movieLocation, movieStart, movieEnd, movieDescription, movieUrl);
        return v;
    }

    private HorizontalLayout createTopBar() {
        HorizontalLayout topBar = new HorizontalLayout();
        topBar.setStyleName("topBar");
        topBar.setSpacing(true);
        caption = new Label();
        caption.setStyleName("main-title");
        topBar.addComponent(caption);
        //topBar.addComponent(createMonthButton());
        //topBar.addComponent(createWeekButton());
        return topBar;
    }

    private Button createWeekButton() {
        Button buttonWeek = new Button("Woche");
        buttonWeek.setStyleName("viewSwitch");
        buttonWeek.addClickListener((Button.ClickListener) event -> showWeek());
        return buttonWeek;
    }

    private void showWeek() {
        setTitle("Diese Woche");
        calendar.setStartDate(getFirstDayOfWeek());
        calendar.setEndDate(getLastDayOfWeek());
    }

    private Button createMonthButton() {
        Button buttonMonth = new Button("Monat");
        buttonMonth.setStyleName("viewSwitch");
        buttonMonth.addClickListener((Button.ClickListener) event -> showMonth());
        return buttonMonth;
    }

    private void showMonth() {
        setTitle("4 Wochen");
        calendar.setStartDate(getFirstDayOfWeek());
        calendar.setEndDate(getNowPlusDays(27));
    }

    private Calendar createCalendarWidget() {
        Calendar calendar = new Calendar();
        calendar.setEventProvider(calendarEventProvider);
        calendar.setCaptionAsHtml(true);
        //calendar.setFirstDayOfWeek(MONDAY);
        calendar.setWidth("100%");
        calendar.setHeight("100%");
        calendar.setFirstVisibleHourOfDay(19);
        calendar.setLastVisibleHourOfDay(24);
        calendar.setHandler(eventClickHander());
        calendar.setHandler((CalendarComponentEvents.DateClickHandler) event -> {
            // do nothing
        });
        return calendar;
    }

    private CalendarComponentEvents.EventClickHandler eventClickHander() {
        return (CalendarComponentEvents.EventClickHandler) event -> {
            CinemaEvent cinemaEvent = (CinemaEvent) event.getCalendarEvent();
            setReadOnlyField(movieTitle, cinemaEvent.getCaption());
            setReadOnlyField(movieLocation, cinemaEvent.getLocation());
            setReadOnlyField(movieDescription, getDescription(cinemaEvent));
            setReadOnlyField(movieStart, formatted(cinemaEvent.getStart()));
            setReadOnlyField(movieEnd, formatted(cinemaEvent.getEnd()));
            if (StringUtils.isNotBlank(cinemaEvent.getUrl())) {
                movieUrl.setVisible(true);
                movieUrl.setResource(new ExternalResource(cinemaEvent.getUrl()));
                movieUrl.setCaption(cinemaEvent.getUrl());
            } else {
                movieUrl.setVisible(false);
            }
        };
    }

    private String getDescription(CinemaEvent cinemaEvent) {
        String caption = cinemaEvent.getCaption().toUpperCase() + " ";
        if (cinemaEvent.getDescription().startsWith(caption)) {
            return cinemaEvent.getDescription().replaceFirst(caption, "");
        } else {
            return cinemaEvent.getDescription();
        }
    }

    private String formatted(Date date) {
        return sf.format(date);
    }

    private void setReadOnlyField(AbstractTextField field, String newValue) {
        field.setReadOnly(false);
        field.setValue(newValue);
        field.setReadOnly(true);
    }

    private void setTitle(final String subTitle) {
        String title = "Programm Freiluftkinos - " + subTitle;
        caption.setValue(title);
        super.getPage().setTitle(title);
    }

    private void updateCalendars() {
        calendarEventProvider.clearCalendars();
        calendarEventProvider.addWebCalendar(kinokalender.fHain(), "theme1");
        calendarEventProvider.addWebCalendar(kinokalender.kberg(), "theme2");
    }

}