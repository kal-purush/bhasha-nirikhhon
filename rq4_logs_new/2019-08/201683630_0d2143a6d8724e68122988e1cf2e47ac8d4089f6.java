package com.intouristing.intouristing.conf;

import com.intouristing.intouristing.filter.WebSocketFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Marcelo Lacroix on 16/08/19.
 */
@Configuration
public class FilterRegistrationConfig {

    @Autowired
    private WebSocketFilter webSocketFilter;

    @Bean
    public FilterRegistrationBean authFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean(webSocketFilter);
        registration.addUrlPatterns("/sockjs/*");
        return registration;
    }
}