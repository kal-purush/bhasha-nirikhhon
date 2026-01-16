package org.wecancodeit.backend.Controllers;

import org.wecancodeit.backend.BOService.UserService;

import jakarta.annotation.Resource;

public class BaseController {

    @Resource
    private UserService userService; // Injected UserService for user operations

    public BaseController(UserService userService) {
        this.userService = userService;
    }

    public UserService getUserService() {
        return userService;
    }

    public boolean checkToken(String authHeader){
        boolean results = false;
        try {
            if (authHeader != null && authHeader.startsWith("Bearer")){
                String token = authHeader.substring(7);
                this.userService.getToken(token);
                results = true;
            }
        } catch (Exception e) {
            return false;
        } 
        return results;
    }
}