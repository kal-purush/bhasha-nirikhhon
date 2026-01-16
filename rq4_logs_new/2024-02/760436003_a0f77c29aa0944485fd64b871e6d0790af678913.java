package com.giexercise.security.controller;

import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;

@Controller
public class SecureController {
    @PreAuthorize("#username == authentication.principal.username")
    public String getMyRoles(String username) {
        //...
        return null;
    }
    @PostAuthorize("#username == authentication.principal.username")
    public String getMyRoles2(String username) {
        //...
        return null;
    }
}