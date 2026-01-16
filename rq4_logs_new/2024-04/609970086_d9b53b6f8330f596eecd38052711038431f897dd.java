package com.bearAndPupperCo.sangenWrestlingApp.Security.DTO;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LoginRequestTest {

    @Test
    void testSettersAndGetters() {
        //Arrange
        LoginRequest request = new LoginRequest();
        request.setUsername("testUser");
        request.setPassword("testPassword");

        //Act & Assert
        assertEquals("testUser", request.getUsername());
        assertEquals("testPassword", request.getPassword());

    }
}