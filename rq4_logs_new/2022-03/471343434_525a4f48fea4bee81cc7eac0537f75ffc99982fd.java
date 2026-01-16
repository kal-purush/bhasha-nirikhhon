package com.example.controller;

import com.example.domain.User;
import com.example.service.MockUserService;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.Arrays;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(UserController.class)
public class UserControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private MockUserService mockUserService;

    @Test
    public void shouldReturnAllUsers() throws Exception {
        User user1 = (new User(1, "A. Mock", true));
        User user2 = (new User(2, "B. Mocking", true));
        User user3 = (new User(3, "M. V. C. Test", false));
        ArrayList<User> users = new ArrayList<User>(
                Arrays.asList(user1, user2, user3));

        when(this.mockUserService.allUsers())
                .thenReturn(users);

        this.mockMvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size()").value(3))
                .andExpect(jsonPath("$[0].id").value(1))
                .andExpect(jsonPath("$[0].name").value("A. Mock"))
                .andExpect(jsonPath("$[0].active").value(true))
                .andExpect(jsonPath("$[1].id").value(2))
                .andExpect(jsonPath("$[1].name").value("B. Mocking"))
                .andExpect(jsonPath("$[1].active").value(true))
                .andExpect(jsonPath("$[2].id").value(3))
                .andExpect(jsonPath("$[2].name").value("M. V. C. Test"))
                .andExpect(jsonPath("$[2].active").value(false));
    }

    @Test
    public void shouldReturnAUserById() throws Exception {
        User user140 = (new User(140, "I. M. Returned", true));

        when(this.mockUserService.findUser(140)).thenReturn(user140);

        this.mockMvc.perform(get("/140"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size()").value(3))
                .andExpect(jsonPath("$.id").value(140))
                .andExpect(jsonPath("$.name").value("I. M. Returned"))
                .andExpect(jsonPath("$.active").value(true));
    }
}