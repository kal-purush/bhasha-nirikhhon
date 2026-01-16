package by.clevertec.aspectlogger.config;

import by.clevertec.aspectlogger.logger.LoggingAspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "exception.handler", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LoggingAspectAutoConfiguration {

    @Bean
    public LoggingAspect loggingAspect() {
        return new LoggingAspect();
    }

}