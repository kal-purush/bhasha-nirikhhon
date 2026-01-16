package com.bearAndPupperCo.sangenWrestlingApp.Security.Controller;

import com.bearAndPupperCo.sangenWrestlingApp.Security.DTO.LoginRequest;
import com.bearAndPupperCo.sangenWrestlingApp.Security.DTO.SignupRequest;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.User;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Service.UserDetailsImpl;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Service.UserService;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Utils.JwtUtils;
import com.bearAndPupperCo.sangenWrestlingApp.TypeAdapters.LocalDateTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseCookie;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class AuthControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDate.class, new LocalDateTypeAdapter())
            .create();

    @MockBean
    AuthenticationManager authenticationManager;

    @MockBean
    private UserService userService;

    @MockBean
    private JwtUtils jwtUtils;


    @Test
    void registerUserSuccess() throws Exception {

        Set<String> roles = new HashSet<>(Arrays.asList("ADMIN", "USER"));
        SignupRequest signUpRequest = new SignupRequest("username","username@email.com", roles, "password");
        User user = new User("username","username@email.com", "password", LocalDate.now());

        String signUpRequestJson = gson.toJson(signUpRequest);
        String userJson = gson.toJson(user);

        when(userService.registerUser(signUpRequest)).thenReturn(user);

        mockMvc.perform(post("/teiai-api/auth/signup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(signUpRequestJson))
                .andExpect(status().isCreated())
                .andExpect(content().json(userJson));

    }

    @Test
    void registerUserError() throws Exception {

        Set<String> roles = new HashSet<>(Arrays.asList("ADMIN", "USER"));
        SignupRequest signUpRequest = new SignupRequest("username","username@email.com", roles, "password");

        String signUpRequestJson = gson.toJson(signUpRequest);


        when(userService.registerUser(signUpRequest)).thenThrow(new RuntimeException());

        mockMvc.perform(post("/teiai-api/auth/signup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(signUpRequestJson))
                .andExpect(status().isBadRequest());
    }

    @Test
    void authenticateUser() throws Exception {

        LoginRequest loginRequest = new LoginRequest("username", "password");
        String loginJson = gson.toJson(loginRequest);

        Authentication mockAuthentication = mock(Authentication.class);

        ResponseCookie cookie = ResponseCookie.from("access_token", "jwtToken").build();

        when(authenticationManager.authenticate(any(UsernamePasswordAuthenticationToken.class))).thenReturn(mockAuthentication);
        when(jwtUtils.generateJwtCookie(any())).thenReturn(cookie);

        mockMvc.perform(post("/teiai-api/auth/signin")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(loginJson))
                .andExpect(status().isOk())
                .andExpect(result -> {
                    String setCookieHeader = result.getResponse().getHeader("Set-Cookie");
                    assertTrue(setCookieHeader.contains("access_token"));
                });
    }

    @Test
    void logoutUser() throws Exception {

        ResponseCookie cookie = ResponseCookie.from("access_token", null).build();

        when(jwtUtils.getCleanJwtCookie()).thenReturn(cookie);

        mockMvc.perform(post("/teiai-api/auth/signout")
                .header("access_token", "jwtToken"))
                .andExpect(status().isOk())
                .andExpect(result -> {
                    String newCookie = result.getResponse().getHeader("access_token");
                    assertNull(newCookie);
                });
    }
}