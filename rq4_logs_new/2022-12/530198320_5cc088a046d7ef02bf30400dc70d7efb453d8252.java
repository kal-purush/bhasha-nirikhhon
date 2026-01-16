package com.schoolmanagement.schoolmanagement.constant;

import org.aspectj.lang.annotation.Before;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class StaticFieldsAndMethodsTest {

    private static final int EXPIRE_TOKEN_AFTER_MINUTES = 5;

    @Test
    void testGenerateOTP() {
        String otp = StaticFieldsAndMethods.generateOTP(4);
        assertEquals(4, otp.length());
    }

    @Test
    void testIsTokenExpired() {
        LocalDateTime tokenCreationDateTime;
        boolean isTokenExpired;

        tokenCreationDateTime = LocalDateTime.now();
        isTokenExpired = StaticFieldsAndMethods.isTokenExpired(tokenCreationDateTime);

        assertFalse(isTokenExpired);

        tokenCreationDateTime = LocalDateTime.now().minusMinutes(EXPIRE_TOKEN_AFTER_MINUTES);
        isTokenExpired = StaticFieldsAndMethods.isTokenExpired(tokenCreationDateTime);

        assertTrue(isTokenExpired);
    }
}