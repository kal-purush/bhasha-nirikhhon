package com.example.projectmrsisa.controller;

import com.example.projectmrsisa.model.Adventure;
import com.example.projectmrsisa.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.*;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AdventureControllerTest {

    private static final String URL_PREFIX = "/adventures";

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
    @WithMockUser(roles = "fishingInstructor")
    public void testGetAdventureById() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/get/" + 3)).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.id").value(3))
                .andExpect(jsonPath("$.name").value("Instrukcije pecanja"))
                .andExpect(jsonPath("$.description").value("Naucite da pecate. Strpljiv i veoma prijatan instruktor"))
                .andExpect(jsonPath("$.price").value(30));
    }

    @Test
    @WithMockUser(roles="fishingInstructor")
    public void testGetAll() throws Exception{
        mockMvc.perform(get(URL_PREFIX + "/getAll")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))//.andExpect(jsonPath("$", hasSize(1))) Trenutno ima 1 avantura, kasnije ce mozda biti vise
                .andExpect(jsonPath("$.[*].id").value(hasItem(3)))
                .andExpect(jsonPath("$.[*].name").value("Instrukcije pecanja"))
                .andExpect(jsonPath("$.[*].description").value("Naucite da pecate. Strpljiv i veoma prijatan instruktor"))
                .andExpect(jsonPath("$.[*].price").value(30.0));
    }

    @Test
    @Transactional
    @Rollback(true) // vrv je defaultno
    @WithMockUser(roles="fishingInstructor")
    public void testDeleteAdventure() throws Exception {
        mockMvc.perform(delete(URL_PREFIX + "/delete-adventure/" + 3)).andExpect(status().isOk());
    }

}