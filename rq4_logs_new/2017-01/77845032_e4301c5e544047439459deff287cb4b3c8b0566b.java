package com.ris;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;

import java.util.Properties;

/**
 * Created by teodor on 02/01/17.
 */
public class DatabasePropertiesListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment environment = event.getEnvironment();
        Properties props = new Properties();
        props.put("spring.data.mongodb.uri", System.getenv("OPENSHIFT_MONGODB_DB_URL") + "locatorapp");
        environment.getPropertySources().addFirst(new PropertiesPropertySource("myProps", props));
    }
}