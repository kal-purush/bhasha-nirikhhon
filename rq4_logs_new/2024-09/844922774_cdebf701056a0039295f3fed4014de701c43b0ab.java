package com.codecool.solarwatch.init;

import com.codecool.solarwatch.model.entity.UserEntity;
import com.codecool.solarwatch.model.user.Role;
import com.codecool.solarwatch.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DataInitializer implements CommandLineRunner {

    private final UserRepository userRepository;

    @Autowired
    public DataInitializer(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    @Transactional
    public void run(String... args) throws Exception {
        if (userRepository.findByUsername("ADMIN").isEmpty()) {
            UserEntity userEntity = new UserEntity();
            userEntity.setUsername("ADMIN");
            userEntity.setPassword("$2a$10$I3fpRqB5FfUSr4ZAr9BdF.I9rtVtKE5xHBC1lr4HiKCeFG6eB0Ogq");
            userEntity.setRole(Role.ROLE_ADMIN);
            userRepository.save(userEntity);
        }
    }
}