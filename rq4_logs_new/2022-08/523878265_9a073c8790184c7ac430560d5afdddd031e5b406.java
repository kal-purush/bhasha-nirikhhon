package org.citybike.journeys;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;


import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc
class JourneyControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void statusOk() throws Exception {
        mockMvc.perform(get("/api/stations"))
                .andExpect(status().isOk());
    }


    @Test
    public void getJourneys() throws Exception {
        MvcResult res = mockMvc.perform(get("/api/journeys"))
                .andReturn();
        String content = res.getResponse().getContentAsString();
        Assertions.assertTrue(content.contains("content"));
    }
}