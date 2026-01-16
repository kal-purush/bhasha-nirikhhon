package org.spiceboys.Travel.Diary.seeder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.spiceboys.Travel.Diary.model.User;
import org.spiceboys.Travel.Diary.repository.UserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
import java.io.InputStream;

@Component
@Order(2)
public class UserSeeder implements CommandLineRunner {
    private final UserRepository userRepository;

    private final ObjectMapper objectMapper;

    public UserSeeder(UserRepository userRepository, ObjectMapper objectMapper) {
        this.userRepository = userRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        if (userRepository.count() == 0) {
            InputStream inputStream = getClass().getResourceAsStream("/data/users.json");
            if (inputStream != null) {
                List<User> users = objectMapper.readValue(inputStream, new TypeReference<List<User>>() {});
                userRepository.saveAll(users);
                System.out.println("✅ Database seeded with " + users.size() + " users");
            }
            else {
                System.out.println("Could not find users.json");
            }
        } else {
            System.out.println("ℹ Database already seeded");
        }
    }

}