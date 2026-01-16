package pl.dchruscinski.config.filter;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FilterConfiguration {
    public static final String API_PATH = "/api/v1";
    @Bean
    public FilterRegistrationBean<LoggerFilter> loggingFilter() {
        FilterRegistrationBean<LoggerFilter> registrationBean
                = new FilterRegistrationBean<>();

        registrationBean.setFilter(new LoggerFilter());
        registrationBean.addUrlPatterns(API_PATH + "/customers/*");
        registrationBean.setOrder(1);

        return registrationBean;
    }
}