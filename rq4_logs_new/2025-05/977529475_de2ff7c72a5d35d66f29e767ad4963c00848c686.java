package com.nexalyze.nexalyze.configuration.security.login.controller;

import com.nexalyze.nexalyze.configuration.json.JsonObject;
import com.nexalyze.nexalyze.configuration.security.login.model.LoginModel;
import com.nexalyze.nexalyze.configuration.security.login.serivce.LoginService;
import com.nexalyze.nexalyze.organization.service.OrganizationService;
import com.nexalyze.nexalyze.user.model.OrganizationUser;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
;

@RequestMapping("/login")
@RestController
public class LoginController {
    @Autowired
    private LoginService loginService;
    @PostMapping("/login")
    public ResponseEntity<JsonObject> login (@RequestBody @Valid LoginModel loginModel){
       JsonObject response = new JsonObject();

       response.addMessage("");
       response.addError("");

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

}