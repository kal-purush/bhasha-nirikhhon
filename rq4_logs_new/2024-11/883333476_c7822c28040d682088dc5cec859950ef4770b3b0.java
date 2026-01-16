package org.rentifytools.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.rentifytools.dto.userDto.UserRequestDto;
import org.rentifytools.dto.userDto.UserResponseDto;
import org.rentifytools.exception.NotFoundException;
import org.rentifytools.service.UserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/users")
public class UserController {
    private final UserService service;

    @GetMapping
    public ResponseEntity<List<UserResponseDto>> getUsers() {
        List<UserResponseDto> users = service.getAllUsers();
        if (users.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(users, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public UserResponseDto getUser(@PathVariable (name = "id") Long id) {
        return service.getUserById(id);
    }

    @PostMapping
    public ResponseEntity<UserResponseDto> createUser(@Valid @RequestBody UserRequestDto user) {
        return new ResponseEntity<>(service.createUser(user), HttpStatus.CREATED);
    }

    @PatchMapping("/{id}")
    public UserResponseDto setRole(@PathVariable (name = "id") Long id, @Valid @RequestParam(name = "role", defaultValue = "USER") String title) {
        return service.setRole(id, title);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<UserResponseDto> deleteUser(@PathVariable (name = "id") Long id) {
        return new ResponseEntity(service.deleteUser(id), HttpStatus.OK);
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<String> userNotFoundException(NotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }
}