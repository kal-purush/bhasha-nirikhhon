package com.flab.kinoistkino.controller;


import com.flab.kinoistkino.model.SearchParam;
import com.flab.kinoistkino.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/delete")
public class DeleteController {
    private final UserService userService;


    @Autowired
    public DeleteController(UserService userService) {
        this.userService = userService;
    }



    @DeleteMapping(path="{account}")
    public void deleteUser(@PathVariable("account") String account, @RequestBody String password) {
            userService.deleteUser(account,password);
    }
}