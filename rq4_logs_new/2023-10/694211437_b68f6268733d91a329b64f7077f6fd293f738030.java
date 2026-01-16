package by.it_academy.team1.usermessages.listeners;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

public class ActiveSessionUsersListener implements HttpSessionAttributeListener {

    public static final String TOTAL_USERS_SESSION_ATTRIBUTE_KEY = "total_users_session_attribute";

    @Override
    public void attributeAdded(HttpSessionBindingEvent event) {
        ServletContext context = event.getSession().getServletContext();
        Object contextValue = context.getAttribute(TOTAL_USERS_SESSION_ATTRIBUTE_KEY);
        if (contextValue == null) {
            context.setAttribute(TOTAL_USERS_SESSION_ATTRIBUTE_KEY, 1);
        } else {
            context.setAttribute(
                    TOTAL_USERS_SESSION_ATTRIBUTE_KEY,
                    Integer.parseInt(contextValue.toString()) + 1
            );
        }
    }

    @Override
    public void attributeRemoved(HttpSessionBindingEvent event) {
        ServletContext context = event.getSession().getServletContext();
        Object contextValue = context.getAttribute(TOTAL_USERS_SESSION_ATTRIBUTE_KEY);
        context.setAttribute(TOTAL_USERS_SESSION_ATTRIBUTE_KEY, Integer.parseInt(contextValue.toString()) - 1);
    }

    @Override
    public void attributeReplaced(HttpSessionBindingEvent event) {

    }
}