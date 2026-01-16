package com.myProject.ProjectSystem.controller;

import com.myProject.ProjectSystem.model.File;
import com.myProject.ProjectSystem.model.User;
import com.myProject.ProjectSystem.service.UserService;
import com.myProject.ProjectSystem.type.FileData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/user")
public class UserController {
   @Autowired
   private UserService userService;
    @PostMapping("/add")
    public String addUser(@RequestBody User user){
        userService.saveUser(user);
        return "New User is added";
    }
    @PostMapping("/addFile")
    public String addFile(@RequestBody FileData fileData){
        userService.addFile(fileData);
        return "New File is added";
    }
    @GetMapping("/getAll")
    public List<User> getAllUsers(){
        return userService.getAllUsers();
    }
    @GetMapping("/getFilesFromUser")
    public List<File> getFiles(@RequestBody User User){
        return userService.getFiles(User);
    }

}