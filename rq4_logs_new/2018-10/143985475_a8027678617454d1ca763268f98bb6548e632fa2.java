package ru.gworkshop.slhub.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import ru.gworkshop.slhub.common.model.repository.UserRepository;
import ru.gworkshop.slhub.common.service.UserHandler;

@Profile( "test" )
@Configuration
@EnableJpaAuditing
public class TestAppConfiguration
{
    @Bean
    UserHandler userHandler(UserRepository userRepository, Environment environment){
        return new UserHandler(userRepository, environment);
    }
}