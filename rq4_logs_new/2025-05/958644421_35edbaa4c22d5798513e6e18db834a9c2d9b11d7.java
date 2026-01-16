package pl.edu.pk.student.kittysecurity.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.edu.pk.student.kittysecurity.dto.user.UserResponseDto;
import pl.edu.pk.student.kittysecurity.services.UserService;

@RestController
@RequestMapping("/api/v1")
public class UserController {

    UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<UserResponseDto> getUserData(@PathVariable Integer userId){
        return userService.getUserData(userId);
    }
}