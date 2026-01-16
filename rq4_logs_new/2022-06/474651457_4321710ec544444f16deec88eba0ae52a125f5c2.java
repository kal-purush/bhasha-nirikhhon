package com.example.projectmrsisa.controller;

import com.example.projectmrsisa.dto.RetreatDTO;
import com.example.projectmrsisa.util.TestUtil;
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

import java.util.ArrayList;

import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestRetreatController {

    private static final String URL_PREFIX = "/retreats";

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
    @WithMockUser(roles = "retreatOwner")
    public void testGetAllRetreats() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/getAll")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.[*].id").value(hasItem(1)))
                .andExpect(jsonPath("$.[*].name").value("Brvnara na Dunavu"))
                .andExpect(jsonPath("$.[*].description").value("Lepo mesto pored reke za kvalitetan odmor. Pogodno za zurke, proslave i drizenja sa prijateljima."))
                .andExpect(jsonPath("$.[*].price").value(50.0));
    }

    @Test
    @WithMockUser(roles = "retreatOwner")
    public void testGetRetreatById() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/get/" + 1)).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("Brvnara na Dunavu"))
                .andExpect(jsonPath("$.description").value("Lepo mesto pored reke za kvalitetan odmor. Pogodno za zurke, proslave i drizenja sa prijateljima."))
                .andExpect(jsonPath("$.price").value(50.0));
    }

    @Test
    @Rollback(true)
    @WithMockUser(roles = "retreatOwner")
    public void testAddRetreat() throws Exception {
        RetreatDTO retreatDTO = new RetreatDTO();
        retreatDTO.setName("Lepa vikendica");
        retreatDTO.setDescription("Opis lepe vikendice");
        retreatDTO.setPrice(30.0);
        retreatDTO.setNumOfRooms(2);
        retreatDTO.setNumOfBeds(3);
        retreatDTO.setCountry("Drzava");
        retreatDTO.setCity("Grad");
        retreatDTO.setStreet("Adresa 1");
        retreatDTO.setPictures(new ArrayList<String>(){ {add("slika1.jpg");}});
        retreatDTO.setRulesOfConduct(new ArrayList<String>(){ {add("Pet friendly");}});
        retreatDTO.setAdditionalServices(new ArrayList<String>(){{add("WiFi");}});

        String json = TestUtil.json(retreatDTO);
        this.mockMvc.perform(post(URL_PREFIX + "/create-retreat").contentType(contentType).content(json)).andExpect(status().isOk());
    }
}