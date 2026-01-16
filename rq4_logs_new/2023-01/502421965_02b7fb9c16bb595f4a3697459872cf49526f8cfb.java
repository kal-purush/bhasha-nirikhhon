package com.raphaelframos.refii.auth;

import com.raphaelframos.refii.profile.ProfileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("refii/auth/")
public class AuthController {

    @Autowired
    private final ProfileService profileService;

    public AuthController(ProfileService profileService) {
        this.profileService = profileService;
    }

    @RequestMapping("{email}")
    public Long userId(@PathVariable("email") String email){
        return profileService.init(email);
    }
}