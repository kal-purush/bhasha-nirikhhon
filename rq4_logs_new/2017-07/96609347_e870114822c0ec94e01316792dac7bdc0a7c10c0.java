package de.thberger.cinevote;

import de.thberger.cinevote.calendar.WebCalendar;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author thb
 */
@Configuration
@ConfigurationProperties( prefix = "cinevote")
@Getter @Setter
public class AppConfig {

    private List<WebCalendarConfig> webCalendars;

    @Getter @Setter
    public static class WebCalendarConfig {
        String name;
        String url;
        String style;
        boolean enabled;
    }
}