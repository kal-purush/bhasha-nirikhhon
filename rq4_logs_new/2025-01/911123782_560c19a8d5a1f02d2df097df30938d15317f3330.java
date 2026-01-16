package com.roomfit.be;

import com.roomfit.be.auth.application.AuthService;
import com.roomfit.be.auth.application.dto.AuthDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class AuthServiceTest {
    @Autowired
    private AuthService authService;

    @Test
    void 인증_서비스(){
        AuthDTO.Login request = new AuthDTO.Login("email","password");
        authService.authenticate(request);
    }
}