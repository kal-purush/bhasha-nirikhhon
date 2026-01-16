package com.example.librarymanagement.controllers;

import com.example.librarymanagement.models.entities.User;
import com.example.librarymanagement.services.IUserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/user")
public class UserController{
    private final IUserService iUserService;

    public UserController(IUserService iUserService) {
        this.iUserService = iUserService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<User> findById(@PathVariable Long id) {
        return new ResponseEntity<>(iUserService.getUserById(id), HttpStatus.OK);
    }

    @GetMapping("/find/email")
    public ResponseEntity<User> findByEmail(@RequestParam(name = "par") String email) {
        return new ResponseEntity<>(iUserService.getUserByEmail(email), HttpStatus.OK);
    }

    @GetMapping("/find/phonenumber")
    public ResponseEntity<User> findByNumber(@RequestParam(name = "par") String number) {
        return new ResponseEntity<>(iUserService.getUserByNumber(number), HttpStatus.OK);
    }

    @PostMapping("/create")
    public ResponseEntity<User> createUser(@RequestBody User user) {
        return new ResponseEntity<>(iUserService.createUser(user), HttpStatus.CREATED);
    }

    @PutMapping("/{id}/update")
    public ResponseEntity<User> updateUser(@PathVariable Long id, @RequestBody User user) {
        return new ResponseEntity<>(iUserService.updateUser(id, user), HttpStatus.OK);
    }

    @DeleteMapping("/{id}/delete")
    public ResponseEntity<Void> deleteUser(@PathVariable Long id) {
        iUserService.deleteUser(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping()
    public ResponseEntity<List<User>> getAllUsers() {
        return new ResponseEntity<>(iUserService.findAll(), HttpStatus.OK);
    }
}