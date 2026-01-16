package com.project.controller;



import com.project.config.JwtUtil;
import com.project.service.UserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(UserController.class)
public class UserControllerTest {
    //Type of test, main entry point for sever-side spring mvc test support
    @Autowired
    private MockMvc mockMvc;

    //bean which you're testing, in this case UserService
    @MockBean
    private UserService userService;

    //expecting JSON output so have to use this
    @MockBean
    private JwtUtil jwtUtil;

    @Test
    public void signup_Success() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders
                .post("/signup")
                .contentType(MediaType.APPLICATION_JSON)
                .content(createUserInJson("newUser", "test123"));

        when(userService.createUser(any())).thenReturn("123456");

        MvcResult result = mockMvc.perform(requestBuilder)
                .andExpect(status().isOk())
                .andExpect(content().json("{\"token\":\"123456\"}"))
                .andReturn();
        System.out.println(result.getResponse().getContentAsString());
    }

    @Test
    public void login_Success() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders
                .post("/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(createUserInJson("newUser", "test123"));

        when(userService.login(any())).thenReturn("123456");

        MvcResult result = mockMvc.perform(requestBuilder)
                .andExpect(status().isOk())
                .andExpect(content().json("{\"token\":\"123456\"}"))
                .andReturn();
        System.out.println(result.getResponse().getContentAsString());
    }

    //The parameters dont have to be the exact json method. Like technically we should be using username instead of name but here it matters that the type is correct, String
    private static String createUserInJson(String username,String password){
        return "{\"username\": \"" + username + "\"," +
                "\"password\": \"" + password + "\"}";
    }
}
