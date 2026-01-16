package com.myproject.bikereviewapp.controller;

import com.myproject.bikereviewapp.entity.Role;
import com.myproject.bikereviewapp.entity.User;
import com.myproject.bikereviewapp.service.abstraction.UserService;
import com.myproject.bikereviewapp.validation.validator.UserUniquenessValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.validation.BindingResult;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AuthController.class)
@AutoConfigureMockMvc(addFilters = false)
class AuthControllerTest {

    @MockBean
    UserService userService;

    @MockBean
    UserUniquenessValidator uniquenessValidator;

    @Autowired
    MockMvc mockMvc;

    @Test
    void signUp_shouldRedirectToLogInPage_ifUserIsValid() throws Exception {

        User validUser = new User(null, "someUsername", "somePassword", false, null, "somePublicName");


        mockMvc.perform(post("/signup")
                        .param("username", validUser.getUsername())
                        .param("password", validUser.getPassword())
                        .param("publicName", validUser.getPublicName())
                ).andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/login"));
    }

    @Test
    void signUp_shouldCreateEnabledUserWithClientRole_ifUserIsValid() throws Exception {

        User validUser = new User(null, "someUsername", "somePassword", false, null, "somePublicName");
        User expectedUser = new User(null, validUser.getUsername(), validUser.getPassword(), true, Role.CLIENT, validUser.getPublicName());

        mockMvc.perform(post("/signup")
                .param("username", validUser.getUsername())
                .param("password", validUser.getPassword())
                .param("publicName", validUser.getPublicName())
        );

        verify(userService).create(expectedUser);
    }

    @Test
    void signUp_shouldCheckUserForUniqueness_ifUserIsValid() throws Exception {

        User validUser = new User(null, "someUsername", "somePassword", false, null, "somePublicName");

        mockMvc.perform(post("/signup")
                .param("username", validUser.getUsername())
                .param("password", validUser.getPassword())
                .param("publicName", validUser.getPublicName())
        );

        verify(uniquenessValidator).validate(any(User.class), any(BindingResult.class));
    }

    @Test
    void signUp_shouldReturnSignUpForm_ifUserIsInvalid() throws Exception {

        User invalidUser = new User(null, "", "", false, null, "somePublicName");

        mockMvc.perform(post("/signup")
                        .param("username", invalidUser.getUsername())
                        .param("password", invalidUser.getPassword())
                        .param("publicName", invalidUser.getPublicName())
                ).andExpect(status().isOk())
                .andExpect(view().name("auth/signup"));
    }

    @Test
    void signUp_shouldReturnFieldErrors_ifUserIsInvalid() throws Exception {

        User invalidUser = new User(null, "", "", false, null, "somePublicName");

        mockMvc.perform(post("/signup")
                        .param("username", invalidUser.getUsername())
                        .param("password", invalidUser.getPassword())
                        .param("publicName", invalidUser.getPublicName())
                ).andExpect(model().attributeHasFieldErrors("user", "username", "password"));
    }

    @Test
    void signUp_shouldNeverCreateUser_ifUserIsInvalid() throws Exception {

        User invalidUser = new User(null, "", "", false, null, "somePublicName");

        mockMvc.perform(post("/signup")
                .param("username", invalidUser.getUsername())
                .param("password", invalidUser.getPassword())
                .param("publicName", invalidUser.getPublicName())
        );

        verify(userService, never()).create(any(User.class));
    }
}