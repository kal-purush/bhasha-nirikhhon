package com.BackendChallenge.TechTrendEmporium.Auth;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import lombok.RequiredArgsConstructor;

import javax.management.ObjectName;

@RestController
@RequestMapping("/api/")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping(value = "login")
    public Object login(@RequestBody LoginRequest request) {
        ResponseEntity<Object> responseEntity = authService.login(request);
        return responseEntity.getBody();
    }


    @PostMapping(value = "auth")
    public Object registerShopper(@RequestBody RegisterShopperRequest request)
    {
        ResponseEntity<Object> responseEntity = authService.registerShopper(request);
        return responseEntity.getBody();
    }

    @PostMapping(value = "admin/auth")
    public Object registerEmployee(@RequestBody RegisterEmployeeRequest request)
    {
        ResponseEntity<Object> responseEntity = authService.registerEmployee(request);
        return responseEntity.getBody();
    }

    @PostMapping(value = "logout")
    public Object logout(@RequestBody LogoutRequest request)
    {
        ResponseEntity<Object> responseEntity = authService.logout(request);
        return responseEntity.getBody();
    }

}