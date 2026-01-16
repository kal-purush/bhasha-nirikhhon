package com.kodilla.ecommerce.controller;

import com.kodilla.ecommerce.domain.enums.StatusUser;
import com.kodilla.ecommerce.dto.UserDto;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/v1/users", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
public class UsersController {

    @GetMapping
    public List<UserDto> getUsers() {
        return Arrays.asList(
                UserDto.builder()
                        .id(1L)
                        .username("Test User")
                        .status(StatusUser.ACTIVE_USER)
                        .userKey(10000L)
                        .build()
        );
    }

    @GetMapping("/{id}")
    public UserDto getUser(@PathVariable Long id) {
        return UserDto.builder()
                .id(1L)
                .username("Test User")
                .status(StatusUser.ACTIVE_USER)
                .userKey(10000L)
                .build();
    }

    @PutMapping("/block")
    public UserDto blockUser(@RequestBody UserDto userDto){
        return UserDto.builder()
                .id(1L)
                .username("Test User")
                .status(StatusUser.BLOCKED_USER)
                .userKey(10000L)
                .build();
    }

    @PutMapping("/{username}/key")
    public UserDto generateUserKey(@PathVariable String username){
        return UserDto.builder()
                .id(1L)
                .username(username)
                .status(StatusUser.ACTIVE_USER)
                .userKey(10001L)
                .build();
    }

    @PostMapping
    public UserDto createUser(@RequestBody UserDto userDto) {
        return UserDto.builder()
                .id(1L)
                .username("Test User")
                .status(StatusUser.ACTIVE_USER)
                .userKey(10000L)
                .build();
    }
}