package com.matrix.matrix_160_demo.controller;

import com.matrix.matrix_160_demo.entity.User;
import com.matrix.matrix_160_demo.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    @GetMapping("/byId")
    public Optional<User> findById(@RequestParam Integer id) {
        return userService.findById(id);
    }

    @GetMapping("/byName")
    public User findByName(@RequestParam String name) {
        return userService.findByName(name);
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/add")
    public User add(@RequestBody User user) {
        return userService.add(user);

    }

    @PutMapping("/update")
    public User update(@RequestBody User user) {
        return userService.update(user);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/delete")
    public void delete(@RequestParam Integer id) {
        userService.delete(id);
    }

}