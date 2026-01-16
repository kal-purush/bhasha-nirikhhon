package com.bin448.backend.AirlineTesting;


import com.bin448.backend.entity.DTOentity.AirlineDTO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.transaction.Transactional;
import java.nio.charset.Charset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AirlineControllerTesting {

    private static final String URL_PREFIX = "/airline";

    private MediaType contentType = new MediaType(MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(), Charset.forName("utf8"));

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void findAirlineTesting() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/findById/10")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.name").value("BIN"));
    }

    @Transactional
    @Rollback(true)
    @Test
    public void addAirlineTesting() throws Exception {
        AirlineDTO airlineDTO = new AirlineDTO();
        airlineDTO.setId(100L);
        airlineDTO.setName("YUG");
        airlineDTO.setOfficeDestination("Beograd");
        airlineDTO.setDescription("...");
        airlineDTO.setAddress("Juga 45");
        airlineDTO.setFlightListID(null);
        airlineDTO.setLuggageID(null);
        airlineDTO.setDiscountTicketListID(null);

        String json = com.bin448.backend.TestUtil.json(airlineDTO);
        this.mockMvc.perform(post(URL_PREFIX+"/create").contentType(contentType).content(json)).andExpect(status().isOk());
    }


    @Transactional
    @Rollback(true)
    @Test
    public void deleteAirlineTesting() throws Exception {
        mockMvc.perform(delete(URL_PREFIX + "/delete/10")).andExpect(status().isOk());
    }

    @Test
    public void findAllTesting() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/findAll")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.[*].id").value(hasItem(10)));
    }

}