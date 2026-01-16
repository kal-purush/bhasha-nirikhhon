package com.luximed.frontservice.config;

import com.luximed.frontservice.model.CurrentUser;
import com.luximed.frontservice.util.UserUtils;
import org.springframework.stereotype.Component;

@Component("thymeleaf")
public class ThymeleafIncludeConfig {
    private final CurrentUser currentUser;

    public ThymeleafIncludeConfig(CurrentUser currentUser) {
        this.currentUser = currentUser;
    }

    public String getCurrentUserPesel(){
        return currentUser.getUserName();
    }
    public String getEmptyUser(){
        return UserUtils.EMPTY_USER;
    }
}