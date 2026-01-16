package com.example.projectmrsisa.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class UserControllerTest {

    private static final String URL_PREFIX = "/users";

    private MediaType contentType = new MediaType(MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype());

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Before
    public void setup(){
        this.mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).apply(springSecurity()).build();
    }

    @Test
    @WithMockUser(roles = "client")
    public void testGetAllInstructors() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/getAllInstructors")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.[*].id").value(hasItem(3)))
                .andExpect(jsonPath("$.[*].email").value("lordje@gmail.com"))
                .andExpect(jsonPath("$.[*].name").value("Djordje"))
                .andExpect(jsonPath("$.[*].surname").value("Vajagic"))
                .andExpect(jsonPath("$.[*].phoneNumber").value("+381617647812"));
    }

    @Test
    @Transactional
    @Rollback(true)
    @WithMockUser(roles="mainAdmin")
    public void testDeleteUser() throws Exception {
        mockMvc.perform(delete(URL_PREFIX + "/delete/" + 5)).andExpect(status().isOk());
    }

    @Test
    @WithMockUser(roles = "mainAdmin")
    public void testFindOwnerOfService() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/findOwnerOfService/" + 3)).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.id").value(3))
                .andExpect(jsonPath("$.email").value("lordje@gmail.com"))
                .andExpect(jsonPath("$.name").value("Djordje"))
                .andExpect(jsonPath("$.surname").value("Vajagic"))
                .andExpect(jsonPath("$.phoneNumber").value("+381617647812"));
    }
}