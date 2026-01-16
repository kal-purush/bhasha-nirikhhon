package com.group_component.master_gateway.service;

import com.group_component.master_gateway.dto.UserDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class AuthService {
    private final RegistrationService registrationService;
    private final LoginService loginService;
    private final LogoutService logoutService;

    public AuthService(RegistrationService registrationService,
                       LoginService loginService,
                       LogoutService logoutService) {
        this.registrationService = registrationService;
        this.loginService = loginService;
        this.logoutService = logoutService;
    }

    public ResponseEntity<?> register(UserDTO userDTO) {
        return this.registrationService.register(userDTO);
    }

    public ResponseEntity<?> login(UserDTO userDTO) {
        return this.loginService.login(userDTO);
    }

    public ResponseEntity<?> logout() {
        return this.logoutService.logout();
    }
}