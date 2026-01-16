package com.moviezone.dai_api;
import com.moviezone.dai_api.controller.AuthenticationController;
import com.moviezone.dai_api.model.dto.TokenDTO;
import com.moviezone.dai_api.model.entity.RefreshToken;
import com.moviezone.dai_api.service.IRefreshTokenService;
import com.moviezone.dai_api.service.IUserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.crypto.SecretKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class AuthenticationControllerTest {

    @InjectMocks
    private AuthenticationController authenticationController;

    @Mock
    private IUserService userService;

    @Mock
    private IRefreshTokenService refreshTokenService;

    @Mock
    private SecretKey secretKey;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void logoutSuccessfully() {
        TokenDTO token = new TokenDTO();
        token.setToken("testToken");

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken("testToken");

        when(refreshTokenService.findByRefreshToken(token.getToken())).thenReturn(refreshToken);

        ResponseEntity<?> response = authenticationController.logout(token);

        verify(refreshTokenService, times(1)).delete(refreshToken);
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
    }

    @Test
    public void refreshTokenSuccessfully() {
        TokenDTO token = new TokenDTO();
        token.setToken("testToken");

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken("testToken");

        when(refreshTokenService.findByRefreshToken(token.getToken())).thenReturn(refreshToken);
        when(refreshTokenService.verifyRefreshTokenExpiration(refreshToken)).thenReturn(refreshToken);

        ResponseEntity<?> response = authenticationController.refreshToken(token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    public void refreshTokenNotFound() {
        TokenDTO token = new TokenDTO();
        token.setToken("testToken");

        when(refreshTokenService.findByRefreshToken(token.getToken())).thenReturn(null);

        ResponseEntity<?> response = authenticationController.refreshToken(token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    public void loginSuccessfully() {
        TokenDTO token = new TokenDTO();
        token.setToken("testToken");

        when(userService.getUser(anyString())).thenReturn(null);

        ResponseEntity<?> response = authenticationController.login(token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    public void loginWithExistingUser() {
        TokenDTO token = new TokenDTO();
        token.setToken("testToken");

        UserDTO user = new UserDTO();
        user.setId("testId");

        when(userService.getUser(anyString())).thenReturn(user);

        ResponseEntity<?> response = authenticationController.login(token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }
}