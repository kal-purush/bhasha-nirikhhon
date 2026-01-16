package com.soteria;

import com.soteria.models.Entity;
import com.soteria.repositories.EntityRepository;
import com.soteria.models.Role;
import com.soteria.repositories.RoleRepository;
import com.soteria.models.User;
import com.soteria.repositories.UserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class DataConfig {
    @Bean
    CommandLineRunner commandLineRunner(EntityRepository entityRepository, RoleRepository roleRepository, UserRepository userRepository) {
        return args -> {
            Entity outlook = new Entity("Outlook", "outlook.com", "");
            Entity gmail = new Entity("Gmail", "gmail.com", "");
            entityRepository.saveAll(List.of(outlook, gmail));

            Role standard = new Role("ROLE_STANDARD");
            Role admin = new Role("ROLE_ADMIN");
            roleRepository.saveAll(List.of(standard, admin));

            User user = new User("root", "toor", new ArrayList<>());
            user.setRole(admin);
            userRepository.save(user);
        };
    }
}