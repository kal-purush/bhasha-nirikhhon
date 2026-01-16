package br.com.study.api.config;

import br.com.study.api.domain.User;
import br.com.study.api.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

@Configuration
@Profile("local")
public class LocalConfig {

    @Autowired
    private UserRepository repository;

    @Bean
    public void startDB() {
        User user = new User(null, "Felipe", "fsdiasdf@gmail.com", "123456");
        User userOther = new User(null, "Fulano", "fulano@gmail.com", "654321");

        repository.saveAll(List.of(user, userOther));

    }
}