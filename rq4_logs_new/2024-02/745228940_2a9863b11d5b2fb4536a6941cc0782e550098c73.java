package org.example.JavaTaigaCode.controllers;

import org.example.JavaTaigaCode.service.Authentication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//@WebMvcTest(AuthController.class)
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class AuthControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @InjectMocks
    private AuthController authController;

    @Mock
    private Authentication authentication;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void loginReturnsOk() throws Exception {
        String username = "testUser";
        String password = "testPassword";

        mockMvc.perform(MockMvcRequestBuilders.post("/api/login")
                        .param("username", username)
                        .param("password", password)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

//    @Test
//    public void loginReturnsError() throws Exception {
//        String username = "testUser";
//        String password = "testPassword";
//
//        mockMvc.perform(MockMvcRequestBuilders.post("/api/login")
//                        .param("username", username)
//                        .param("password", password)
//                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
//                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
//    }

    @Test
    public void testLogin_Successful() {
        // Arrange
        String username = "testUser";
        String password = "testPassword";
        int expectedMemberId = 123;
        when(authentication.authenticate(username, password)).thenReturn(expectedMemberId);

        // Act
        Integer memberId = authController.login(username, password);

        // Assert
        assertEquals(expectedMemberId, memberId);
        verify(authentication).authenticate(username, password);
    }

    @Test
    public void testLogin_Failure() {
        // Arrange
        String username = "testUser";
        String password = "testPassword";
        when(authentication.authenticate(username, password)).thenReturn(null);

        // Act
        Integer memberId = authController.login(username, password);

        // Assert
        assertNull(memberId);
        verify(authentication).authenticate(username, password);
    }


}