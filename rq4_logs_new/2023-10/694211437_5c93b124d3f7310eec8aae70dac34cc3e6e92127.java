package by.it_academy.team1.usermessages.listeners;

import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

import java.util.Arrays;

public class DefaultDataInitializer implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        var servletContext = sce.getServletContext();
        servletContext.log("Default listener activation");
        try {Class.forName("org.postgresql.Driver");}
        catch (ClassNotFoundException exception) {
            System.out.println(Arrays.toString(exception.getStackTrace()));
        }
    }
}