package com.rest.server.controller;

import com.rest.server.helper.UsersDTO;
import com.rest.server.service.UsersService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@CrossOrigin("*")
@RequestMapping(value = "/app/users")
public class UsersController {

    @Autowired
    private UsersService usersService;

    @PostMapping(value = "/save")
    protected ResponseEntity<Object>save(@RequestBody UsersDTO usersDTO){
        try {
            return new ResponseEntity<Object>(usersService.save(usersDTO), HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "/uploadProfilePic")
    protected ResponseEntity<Object>uploadProfilePicture(@RequestParam("email") String email, @RequestParam("file")MultipartFile file){
        try {
            return new ResponseEntity<Object>(usersService.setProfile(email,file),HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "/veryfy")
    protected ResponseEntity<Object>accountVeryfy(@RequestParam("code") String code){
        try {
            return new ResponseEntity<Object>(usersService.accountVeryfy(code),HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value = "/search/{email}")
    protected ResponseEntity<Object>search(@PathVariable("email") String email){
        try {
            return new ResponseEntity<Object>(usersService.search(email),HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping(value = "/login/{email}/{password}")
    protected ResponseEntity<Object>login(@PathVariable("email")String email,@PathVariable("password")String password){
        try {
            return new ResponseEntity<Object>(usersService.login(email,password),HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}