package com.myproject.bikereviewapp.validation.validator;

import com.myproject.bikereviewapp.entity.User;
import com.myproject.bikereviewapp.service.abstraction.UserService;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindingResult;

@Component
public class UserUniquenessValidator {

    private final UserService userService;

    public UserUniquenessValidator(UserService userService) {
        this.userService = userService;
    }

    public void validate(User user, BindingResult bindingResult) {

        if (userService.exists(user.getUsername())) {
            bindingResult.rejectValue("username", "error.user", "User with same username is already exists.");
        }
    }
}