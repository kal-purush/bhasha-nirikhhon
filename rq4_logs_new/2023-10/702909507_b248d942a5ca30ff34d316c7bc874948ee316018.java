package ru.springboot.init;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.springboot.model.User;
import ru.springboot.userServise.UserService;
@Component
public class Init {
    private final UserService userService;
    @Autowired
    public Init(UserService userService) {
        this.userService = userService;
    }
    @PostConstruct
    public void initialDataBase() {
        userService.addUser(new User("Igor", "Ostrovsky", (byte) 52, "Russian"));
    }
}