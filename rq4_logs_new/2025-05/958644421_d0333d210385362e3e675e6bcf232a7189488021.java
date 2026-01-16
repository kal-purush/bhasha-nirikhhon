package pl.edu.pk.student.kittysecurity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import pl.edu.pk.student.kittysecurity.dto.JwtResponseDto;
import pl.edu.pk.student.kittysecurity.dto.LoginRequestDto;
import pl.edu.pk.student.kittysecurity.entity.RefreshToken;
import pl.edu.pk.student.kittysecurity.repository.UserRepository;
import pl.edu.pk.student.kittysecurity.services.AuthService;
import pl.edu.pk.student.kittysecurity.services.JwtService;
import pl.edu.pk.student.kittysecurity.services.RefreshTokenService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
public class AuthServiceTests {

    @Mock
    private AuthenticationManager authManager;

    @Mock
    private JwtService jwtService;

    @Mock
    private RefreshTokenService refreshTokenService;

    @InjectMocks
    private AuthService authService;


    @Test
    public void loginUserSuccess() {
        String username = "testUser";
        String password = "testPass";
        String jwtToken = "mocked-jwt-token";
        String refreshToken = "mocked-refresh-token";

        LoginRequestDto request = new LoginRequestDto(username, password);

        Mockito.when(authManager.authenticate(Mockito.any(UsernamePasswordAuthenticationToken.class))).thenReturn(Mockito.mock(Authentication.class));

        Mockito.when(jwtService.generateToken(username)).thenReturn(jwtToken);

        RefreshToken mockRefreshToken = new RefreshToken();
        mockRefreshToken.setRawToken("mocked-refresh-token");

        Mockito.when(refreshTokenService.createRefreshToken(anyString())).thenReturn(mockRefreshToken);

        ResponseEntity<JwtResponseDto> response = authService.verify(request);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Bearer", response.getBody().getAccessTokenType());
        assertEquals(jwtToken, response.getBody().getAccessToken());
        assertEquals(refreshToken, response.getBody().getRefreshToken());
    }

    @Test
    public void shouldThrowUserFailExceptionWhenCredentialsIncorrect() {
        String username = "wrongUser";
        String password = "wrongPassword";

        LoginRequestDto request = new LoginRequestDto(username, password);

        Mockito.when(authManager.authenticate(Mockito.any(UsernamePasswordAuthenticationToken.class))).thenThrow(new BadCredentialsException("Bad credentials"));

        assertThrows(AuthenticationException.class, () -> {
            authService.verify(request);
        });
    }
}