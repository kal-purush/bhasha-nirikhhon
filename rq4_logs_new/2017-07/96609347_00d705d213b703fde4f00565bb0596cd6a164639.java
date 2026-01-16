package de.thberger.cinevote.ui;

import com.vaadin.server.VaadinSession;
import de.thberger.cinevote.calendar.CinemaEvent;

/**
 * @author thb
 */
public class UserSession {

    private static final String SELECTED_ITEM = "SELECTED_ITEM";

    public static void setSelectedEvent(CinemaEvent event) {
        try {
            VaadinSession.getCurrent().getLockInstance().lock();
            VaadinSession.getCurrent().setAttribute(SELECTED_ITEM, event);
        } finally {
            VaadinSession.getCurrent().getLockInstance().unlock();
        }
    }

    public static CinemaEvent getSelectedItem() {
        try {
            VaadinSession.getCurrent().getLockInstance().lock();
            return (CinemaEvent) VaadinSession.getCurrent().getAttribute(SELECTED_ITEM);
        } finally {
            VaadinSession.getCurrent().getLockInstance().unlock();
        }
    }


}