package com.project.controller;

import com.project.model.UserProfile;
import com.project.service.UserProfileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserProfileController {

    @Autowired
    UserProfileService userProfileService;

    @PostMapping("/profile/{username}")
    public UserProfile createUserProfile(@PathVariable String username, @RequestBody UserProfile newUserProfile) {
        return userProfileService.createUserProfile(username, newUserProfile);
    }
}