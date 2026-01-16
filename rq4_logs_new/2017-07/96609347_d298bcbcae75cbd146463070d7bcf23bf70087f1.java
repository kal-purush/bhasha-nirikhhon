package de.thberger.cinevote.ui;

import com.vaadin.data.Property;
import com.vaadin.spring.annotation.SpringComponent;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import de.thberger.cinevote.AppConfig;
import de.thberger.cinevote.calendar.WebCalendar;
import de.thberger.cinevote.calendar.WebCalendarEventProvider;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;

/**
 * @author thb
 */
@SpringComponent
@AllArgsConstructor
@Slf4j
public class SelectionBar extends HorizontalLayout {

    private WebCalendarEventProvider eventProvider;

    @PostConstruct
    private void init() {
        setSpacing(true);
        setStyleName("selectionBar");
        addComponent(new Label("Angezeigte Kinos:"));
        addBoxes();
    }

    private void addBoxes() {
        for (WebCalendar calendar : eventProvider.getCalendars()) {
            AppConfig.WebCalendarConfig calendarConfig = calendar.getConfig();
            CheckBox checkBox = new CheckBox(calendarConfig.getShortName());
            checkBox.setStyleName(calendar.getConfig().getStyle());
            checkBox.setValue(calendarConfig.isVisible());
            checkBox.addValueChangeListener(new CheckboxChangeListener(calendar));
            addComponent(checkBox);
        }
    }


    @AllArgsConstructor
    private class CheckboxChangeListener implements Property.ValueChangeListener {

        private WebCalendar calendar;

        @Override
        public void valueChange(Property.ValueChangeEvent event) {
            boolean visible = (boolean) event.getProperty().getValue();
            calendar.getConfig().setVisible(visible);
            eventProvider.setVisibility(calendar, visible);
        }
    }
}