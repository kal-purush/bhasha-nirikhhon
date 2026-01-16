package by.it.medved.controllers;

import by.it.medved.dto.UserRequest;
import by.it.medved.dto.UserResponse;
import by.it.medved.enums.Role;
import by.it.medved.services.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/userSave")
    public UserResponse saveUser(@RequestBody UserRequest userRequest) {
        return userService.saveUser(userRequest);
    }

    @GetMapping("/userById/{id}")
    public UserResponse getUserById(@PathVariable Long id) {
        return userService.getUserById(id);
    }

    @GetMapping("/userByLogin/{login}")
    public UserResponse getUserByLogin(@PathVariable String login) {
        return userService.getUserByLogin(login);
    }

    @GetMapping("/users")
    public List<UserResponse> getUsers() {
        return userService.getUsers();
    }

    @GetMapping("/userUpdateRole/{id}/{role}")
    public UserResponse updateRole(@PathVariable Long id, @PathVariable Role role) {
        return userService.updateRole(id, role);
    }

    @GetMapping("/userDelete/{id}")
    public void deleteUser(@PathVariable Long id) {
        userService.deleteUser(id);
    }
}