package com.example.backend.controllers;

import com.example.backend.dtos.AsteroidOutgoingDTO;
import com.example.backend.dtos.PowerUpDTO;
import com.example.backend.services.AsteroidService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class AsteroidControllerTest {

    private MockMvc mockMvc;

    @Mock
    private AsteroidService asteroidService;

    @InjectMocks
    private AsteroidController asteroidController;

    @BeforeEach
    public void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(asteroidController).build();
    }

    @Test
    public void getAsteroidData_ReturnsData() throws Exception {
        int width = 100;
        int height = 100;
        AsteroidOutgoingDTO dto = new AsteroidOutgoingDTO(null, new ArrayList<>() );
        given(asteroidService.getAsteroidData(width, height)).willReturn(dto);

        mockMvc.perform(get("/api/asteroid/{width}/{height}", width, height))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    @Test
    public void getAsteroidData_WithInvalidParams_ReturnsBadRequest() throws Exception {
        int width = -1;
        int height = -1;
        AsteroidOutgoingDTO dto = new AsteroidOutgoingDTO("Invalid dimensions");
        given(asteroidService.getAsteroidData(width, height)).willReturn(dto);

        mockMvc.perform(get("/api/asteroid/{width}/{height}", width, height))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.error").value("Invalid dimensions"));
    }

    @Test
    public void getPowerUpData_ReturnsData() throws Exception {
        int width = 100;
        int height = 100;
        PowerUpDTO dto = new PowerUpDTO(null, new ArrayList<>());
        given(asteroidService.getPowerUpData(width, height)).willReturn(dto);

        mockMvc.perform(get("/api/power-up/{width}/{height}", width, height))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    @Test
    public void getPowerUpData_WithInvalidParams_ReturnsBadRequest() throws Exception {
        int width = -1;
        int height = -1;
        PowerUpDTO dto = new PowerUpDTO("Invalid dimensions");
        given(asteroidService.getPowerUpData(width, height)).willReturn(dto);

        mockMvc.perform(get("/api/power-up/{width}/{height}", width, height))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.error").value("Invalid dimensions"));
    }
}