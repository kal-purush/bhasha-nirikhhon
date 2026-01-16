package com.bin448.backend.HotelTesting;

import com.bin448.backend.entity.DTOentity.HotelDTO;
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

@RunWith(SpringRunner.class)
@SpringBootTest
public class HotelControllerTesting {

    private static final String URL_PREFIX = "/hotel";

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
    public void testGetHotel() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/1")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.name").value("Ninas hotel"))
                .andExpect(jsonPath("$.addressId").value(1));
    }

    @Transactional
    @Rollback(true)
    @Test
    public void testAddHotel() throws Exception {
        HotelDTO hotelDTO = new HotelDTO();
        hotelDTO.setAddressId(1l);
        hotelDTO.setHotel_id(1l);
        hotelDTO.setName("Sun");

        String json = com.bin448.backend.TestUtil.json(hotelDTO);
        this.mockMvc.perform(post(URL_PREFIX).contentType(contentType).content(json)).andExpect(status().isOk());
    }

    @Test
    public void testGetMenu() throws Exception {
        MvcResult result = mockMvc.perform(get(URL_PREFIX + "/getMenu/1")).andExpect(status().isOk()).andReturn();
        int length = result.getResponse().getContentLength();
        assertThat(length).isEqualTo(0);
    }

    @Test
    public void testGetDescription() throws Exception {
        MvcResult result = mockMvc.perform(get(URL_PREFIX + "/getDescription/1")).andExpect(status().isOk()).andReturn();
        String description = result.getResponse().getContentAsString();
        assertThat(description).isEqualTo("Beautiful peacefull place");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testAddMenuItem() throws Exception {
        MvcResult result = mockMvc.perform(get(URL_PREFIX + "/addMenuItem/" + "WiFi" + "/" + 12 + "/1")).andExpect(status().isOk()).andReturn();
        String description = result.getResponse().getContentAsString();
        assertThat(description).isEqualTo("Menu item is successfully added");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testRemoveMenuItem() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/addMenuItem/" + "WiFi" + "/" + 12 + "/1")).andExpect(status().isOk());
        MvcResult result = mockMvc.perform(get(URL_PREFIX + "/removeMenuItem/" + "WiFi" + "/1")).andExpect(status().isOk()).andReturn();
        String description = result.getResponse().getContentAsString();
        assertThat(description).isEqualTo("Menu item is successfully removed");
    }

    @Test
    public void testFindAll() throws Exception {
        mockMvc.perform(get(URL_PREFIX)).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.[*].hotel_id").value(hasItem(1)))
                .andExpect(jsonPath("$.[*].hotel_id").value(hasItem(4)));
    }

    @Test
    public void testFindAllAddresses() throws Exception {
        mockMvc.perform(get(URL_PREFIX + "/addresses")).andExpect(status().isOk())
                .andExpect(content().contentType(contentType))
                .andExpect(jsonPath("$.[*].id").value(hasItem(1)))
                .andExpect(jsonPath("$.[*].id").value(hasItem(2)));
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testRemoveHotel() throws Exception {
        this.mockMvc.perform(put(URL_PREFIX + "/remove/1")).andExpect(status().isOk());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testChangeHotel() throws Exception {
        HotelDTO hotelDTO = new HotelDTO();
        hotelDTO.setAddressId(1l);
        hotelDTO.setName("Moon");
        String json = com.bin448.backend.TestUtil.json(hotelDTO);
        this.mockMvc.perform(put(URL_PREFIX + "/1").contentType(contentType).content(json)).andExpect(status().isOk());
    }

}