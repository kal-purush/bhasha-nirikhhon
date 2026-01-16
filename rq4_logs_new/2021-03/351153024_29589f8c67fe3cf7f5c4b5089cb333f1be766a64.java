package com.harrybro.security.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;

@WebMvcTest
public class UserAccessTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private PasswordEncoder passwordEncoder;

    UserDetails user1() {
        return User.builder()
                .username("user1")
                .password(passwordEncoder.encode("1234"))
                .roles("USER")
                .build();
    }

    UserDetails admin() {
        return User.builder()
                .username("admin")
                .password(passwordEncoder.encode("1234"))
                .roles("ADMIN")
                .build();
    }

    @Test
    @DisplayName("The user can access the user page.")
//    @WithMockUser(username = "user1", roles = {"USER"})
    void theUserCanAccessTheUserPage() throws Exception {
//        String response = mockMvc.perform(MockMvcRequestBuilders.get("/user"))
        String response = mockMvc.perform(MockMvcRequestBuilders.get("/user").with(user(user1())))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn().getResponse().getContentAsString();

        SecurityMessage message = mapper.readValue(response, SecurityMessage.class);
        Assertions.assertEquals("user page", message.getMessage());
    }

    @Test
    @DisplayName("The user can not access the admin page.")
    @WithMockUser(username = "user1", roles = {"USER"})
    void theUserCanNotAccessTheAdminPage() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/admin"))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError());
    }

    @Test
    @DisplayName("The admin can access the user page and the admin page.")
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    void theAdminCanAccessTheUserPageAndTheAdminPage() throws Exception {
        String contentAsStringUser = mockMvc.perform(MockMvcRequestBuilders.get("/user"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn().getResponse().getContentAsString();
        SecurityMessage userMessage = mapper.readValue(contentAsStringUser, SecurityMessage.class);
        Assertions.assertEquals("user page", userMessage.getMessage());

        String contentAsStringAdmin = mockMvc.perform(MockMvcRequestBuilders.get("/admin"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn().getResponse().getContentAsString();
        SecurityMessage adminMessage = mapper.readValue(contentAsStringAdmin, SecurityMessage.class);
        Assertions.assertEquals("admin page", adminMessage.getMessage());
    }

    @Test
    @DisplayName("The homepage cannot be accessed by users who are not logged in.")
    void theHomepageCannotBeAccessedByUsersWhoAreNotLoggedIn() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/"))
                .andExpect(MockMvcResultMatchers.status().is3xxRedirection());
        mockMvc.perform(MockMvcRequestBuilders.get("/user"))
                .andExpect(MockMvcResultMatchers.status().is3xxRedirection());
        mockMvc.perform(MockMvcRequestBuilders.get("/admin"))
                .andExpect(MockMvcResultMatchers.status().is3xxRedirection());
    }

}