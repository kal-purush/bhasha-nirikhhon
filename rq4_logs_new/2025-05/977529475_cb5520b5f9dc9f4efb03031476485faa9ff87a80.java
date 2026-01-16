package com.nexalyze.nexalyze.user.controller;

import com.nexalyze.nexalyze.user.model.OrganizationUser;
import com.nexalyze.nexalyze.user.model.OrganizationUserModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/organizationuser")
public class OrganizationUserController {

    @GetMapping("/user")
    public ResponseEntity<OrganizationUserModel> getUserdata (@AuthenticationPrincipal OrganizationUser user) {
        OrganizationUserModel userModel = new OrganizationUserModel();
        userModel.setFirstname(user.getFirstname());
        userModel.setSurname(user.getSurname());
        userModel.setEmail(user.getEmail());
        userModel.setRole(user.getRole());
        userModel.setUsername(user.getUsername());
        return ResponseEntity.status(HttpStatus.CREATED).body(userModel);
    }
}