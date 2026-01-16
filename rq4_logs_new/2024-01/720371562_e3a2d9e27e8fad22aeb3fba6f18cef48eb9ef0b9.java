package com.example.backend.controllers;

import com.example.backend.dtos.UserDetailsDTO;
import com.example.backend.security.jwt.JwtUtil;
import com.example.backend.security.models.AuthenticationRequest;
import com.example.backend.services.UserService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;




@SpringBootTest
@AutoConfigureMockMvc
class UserControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AuthenticationManager authenticationManager;

    @MockBean
    private JwtUtil jwtUtil;

    @MockBean
    private UserService userService;

    @BeforeEach
    void setUp() {
        UserDetailsDTO defaultUserDetails = new UserDetailsDTO();
        defaultUserDetails.setEnabled(true);
        defaultUserDetails.setUsername("user@user.at");
        defaultUserDetails.setPassword("user1234");
        defaultUserDetails.setDeleted(false);
        when(userService.loadUserByUsername(anyString())).thenReturn(defaultUserDetails);
    }

    /*
    // this should be handeled by SpringBoot
    @AfterEach
    void tearDown() {
    }*/

    @Test
    public void whenLoginSuccessfulThenReturnJwt() throws Exception {
        AuthenticationRequest request = new AuthenticationRequest("user@user.at", "user1234");
        String expectedToken = "token";
        when(jwtUtil.generateToken(any(UserDetailsDTO.class))).thenReturn(expectedToken);

        mockMvc.perform(post("/api/users/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jwt").value(expectedToken));
    }

    @Test
    public void whenLoginFailsThenReturnBadRequest() throws Exception {
        AuthenticationRequest request = new AuthenticationRequest("user@user.at", "user1234");
        when(authenticationManager.authenticate(any())).thenThrow(BadCredentialsException.class);

        mockMvc.perform(post("/api/users/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

}