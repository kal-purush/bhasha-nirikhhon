package pl.edu.pk.student.kittysecurity.services;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.pk.student.kittysecurity.entity.User;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class JwtServiceTests {

    private JwtService jwtService;

    @BeforeEach
    void setUp() {
        jwtService = new JwtService();
    }

    @Test
    void shouldGenerateToken() {
        Integer userId = 123;

        String token = jwtService.generateToken(userId);

        assertNotNull(token);
        assertFalse(token.isEmpty());
    }

    @Test
    void shouldExtractUserIdFromToken() {
        Integer userId = 456;
        String token = jwtService.generateToken(userId);

        String extractedUserId = jwtService.extractUserId(token);

        assertEquals(String.valueOf(userId), extractedUserId);
    }

    @Test
    void shouldValidateValidToken() {
        Integer userId = 789;
        String token = jwtService.generateToken(userId);

        User user = new User();
        user.setId(userId);

        boolean isValid = jwtService.validateToken(token, user);

        assertTrue(isValid);
    }

    @Test
    void shouldDetectTokenExpirationCorrectly() {
        Integer userId = 1;
        String token = jwtService.generateToken(userId);

        Date expirationDate = jwtService.extractExpiration(token);

        assertNotNull(expirationDate);
        assertTrue(expirationDate.after(new Date()));
    }
}