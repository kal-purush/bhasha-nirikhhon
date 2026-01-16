package org.example.todoapp.controllers;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.*;
import org.example.todoapp.dto.*;
import org.example.todoapp.services.*;
import org.example.todoapp.utils.*;
import org.mockito.*;
import org.springframework.http.*;
import org.springframework.security.authentication.*;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.setup.*;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class AuthControllerTest {

    @Mock
    private UserService userService;

    @Mock
    private JwtTokenUtils jwtTokenUtils;

    @Mock
    private AuthenticationManager authenticationManager;

    @InjectMocks
    private AuthController authController;

    private MockMvc mockMvc;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(authController).build();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void singUp_ShouldReturnCreateStatus() throws Exception {
        var userRegisterDto = new UserRegisterDto(
            "test@example.com",
            "Test User",
            "password"
        );

        doNothing().when(userService).createUser(userRegisterDto);
        ResponseEntity<?> response = authController.signUp(userRegisterDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        verify(userService, times(1)).createUser(userRegisterDto);
    }

    @Test
    void signIn() throws Exception {

    }

}