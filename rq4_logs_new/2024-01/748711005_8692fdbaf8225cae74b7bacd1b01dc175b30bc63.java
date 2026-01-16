package com.example.demo.controller;

import com.example.demo.base.BaseController;
import com.example.demo.base.StructureRS;
import com.example.demo.model.request.auth.ChangePasswordRQ;
import com.example.demo.service.user.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * @author Sattya
 * create at 1/31/2024 7:59 AM
 */
@RestController
@RequestMapping("api/v1/users")
@RequiredArgsConstructor
public class UserController extends BaseController {
    private final UserService userService;
    @ResponseStatus(HttpStatus.OK)
    @PutMapping("/change-password")
    public StructureRS changePassword(@RequestBody @Validated ChangePasswordRQ request){
        return response(userService.changePassword(request)).getBody();
    }

    @GetMapping("/my-profile")
    public StructureRS myProfile(){
        return response(userService.myProfile()).getBody();
    }
}