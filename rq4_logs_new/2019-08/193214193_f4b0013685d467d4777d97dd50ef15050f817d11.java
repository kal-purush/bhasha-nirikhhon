package hillel.spring;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import lombok.val;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class TestContext {
    @Primary
    @Bean
    public Clock testClock(){
        val instant = LocalDateTime.parse("2010-01-01T12:00:00").toInstant(ZoneOffset.UTC);
        return Clock.fixed(instant, ZoneId.systemDefault());
    }
}