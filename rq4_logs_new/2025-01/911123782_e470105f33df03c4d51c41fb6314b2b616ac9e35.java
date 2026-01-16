package com.roomfit.be.user;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/user")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    @PostMapping("")
    public UserDTO.Response create(@RequestBody() UserDTO.Create request){
        return userService.create(request);
    }

    @GetMapping("")
    public List<UserDTO.Response> readAll(){
        return userService.readAll();
    }

    @GetMapping("/{user_id}")
    public UserDTO.Response readById(@PathVariable("user_id") Long id) {
        return userService.readById(id);
    }

}