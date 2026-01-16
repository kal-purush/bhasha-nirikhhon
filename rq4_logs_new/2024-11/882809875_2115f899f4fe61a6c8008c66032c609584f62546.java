package org.wecancodeit.backend.BOService;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.wecancodeit.backend.DataModels.UserModel;
import org.wecancodeit.backend.Repositories.UserRepository;

import jakarta.annotation.Resource;

@Component
public class Populator implements CommandLineRunner {

    @Resource
    private final UserRepository users;

    public Populator(UserRepository users) {
        this.users = users;
    }

    @Override
    public void run(String... args) throws Exception {

        UserModel user = new UserModel(1L, "testUser1", "password123", 10);
        users.save(user);

        user = new UserModel(2L, "testUser2", "securePass456", 12);
        users.save(user);

        user = new UserModel(3L, "testUser3", "mySecret789", 8);
        users.save(user);

        user = new UserModel(4L, "testUser4", "tootiFrooti123", 9);
        users.save(user);

        user = new UserModel(5L, "testUser5", "tedBundy124", 6);
        users.save(user);
    }

}