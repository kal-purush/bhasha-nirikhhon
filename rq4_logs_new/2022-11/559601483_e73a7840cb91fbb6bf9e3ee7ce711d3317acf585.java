package com.soteria.user;

import com.soteria.controllers.UserController;
import com.soteria.models.User;
import com.soteria.security.jwt.JwtTokenProvider;
import com.soteria.services.RoleService;
import com.soteria.services.UserService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(SpringExtension.class)
@WebMvcTest(UserController.class)
public class UserControllerTest {
    @MockBean
    UserService userService;

    @MockBean
    RoleService roleService;

    @MockBean
    JwtTokenProvider jwtTokenProvider;

    @MockBean
    AuthenticationManager authenticationManager;

    @MockBean
    PasswordEncoder passwordEncoder;

    @Autowired
    MockMvc mockMvc;

    @Test
    public void testSingUp() throws Exception {
        User user = new User("userTest", passwordEncoder.encode("password"), new ArrayList<>());

        Mockito.when(userService.addUser(user)).thenReturn(user);

        mockMvc.perform(post("/sign-in"))
                .andExpect(status().isOk())
                .andExpect((ResultMatcher) jsonPath("$", Matchers.hasSize(1)))
                .andExpect((ResultMatcher) jsonPath("$[0].userName", Matchers.is("root")));
    }

    @Test
    public void testFindAll() throws Exception {
        User user = new User("userTest", passwordEncoder.encode("password"), new ArrayList<>());
        List<User> users = List.of(user);

        Mockito.when(userService.getUsers()).thenReturn(users);

        mockMvc.perform(get("/all"))
                .andExpect(status().isOk())
                .andExpect((ResultMatcher) jsonPath("$", Matchers.hasSize(1)))
                .andExpect((ResultMatcher) jsonPath("$[0].userName", Matchers.is("root")));
    }
}