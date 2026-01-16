package org.example.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.domain.dto.UserDTO;
import org.example.domain.model.User;
import org.example.domain.model.UserRole;
import org.example.mapper.UserMapper;
import org.example.service.AuthServiceJdbc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class AuthControllerTest {

    private MockMvc mockMvc;

    @Mock
    private AuthServiceJdbc authService;

    @Mock
    private UserMapper userMapper;

    @InjectMocks
    private AuthController authController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(authController).build();
    }

    @Test
    public void testRegisterSuccess() throws Exception {
        UserDTO userDTO = new UserDTO(1, "username", "password", "email@example.com", 25, UserRole.CUSTOMER);

        when(authService.register(any(UserDTO.class))).thenReturn(true);

        mockMvc.perform(post("/api/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(userDTO)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$").value("Register successful."));
    }

    @Test
    public void testRegisterConflict() throws Exception {
        UserDTO userDTO = new UserDTO(1, "username", "password", "email@example.com", 25, UserRole.CUSTOMER);

        when(authService.register(any(UserDTO.class))).thenReturn(false);

        mockMvc.perform(post("/api/auth/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(userDTO)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$").value("Username already in use."));
    }

    @Test
    public void testLoginSuccess() throws Exception {
        UserDTO userDTO = new UserDTO(1, "username", "password", "email@example.com", 25, UserRole.CUSTOMER);
        User user = new User(1, "username", "password", "email@example.com", 25, UserRole.CUSTOMER, null);

        when(authService.login(any(String.class), any(String.class))).thenReturn(userDTO);
        when(userMapper.toEntity(any(UserDTO.class))).thenReturn(user);

        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(userDTO)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.username").value("username"))
                .andExpect(jsonPath("$.email").value("email@example.com"));
    }

    @Test
    public void testLoginUnauthorized() throws Exception {
        UserDTO userDTO = new UserDTO(1, "username", "password", "email@example.com", 25, UserRole.CUSTOMER);

        when(authService.login(any(String.class), any(String.class))).thenReturn(null);

        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new ObjectMapper().writeValueAsString(userDTO)))
                .andExpect(status().isUnauthorized());
    }
}