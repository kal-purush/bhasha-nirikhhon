package by.it_academy.team1.usermessages.listeners;


import by.it_academy.team1.usermessages.service.api.IStatisticsService;
import by.it_academy.team1.usermessages.service.database.DatabaseStatisticsService;
import by.it_academy.team1.usermessages.service.memory.MemoryStatisticsService;
import jakarta.servlet.http.HttpSessionAttributeListener;
import jakarta.servlet.http.HttpSessionBindingEvent;

public class MemoryActiveSessionUsersListener implements HttpSessionAttributeListener {
    private final IStatisticsService statisticsService = MemoryStatisticsService.getInstance();


    @Override
    public void attributeAdded(HttpSessionBindingEvent event) {
        if("user".equalsIgnoreCase(event.getName()) && event.getValue() != null){
        var servletContext = event.getSession().getServletContext();
        servletContext.log("attributeAdded: " + event.getName() + event.getValue());
            this.statisticsService.incSessionCount();
        }
    }

    @Override
    public void attributeRemoved(HttpSessionBindingEvent event) {
        if("user".equalsIgnoreCase(event.getName())){
            this.statisticsService.decSessionCount();
        }
    }

    @Override
    public void attributeReplaced(HttpSessionBindingEvent event) {

    }
}