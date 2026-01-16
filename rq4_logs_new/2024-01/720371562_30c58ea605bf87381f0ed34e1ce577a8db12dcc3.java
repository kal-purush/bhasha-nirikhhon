package com.example.backend.controllers;

import com.example.backend.models.AlmostCityEntity;
import com.example.backend.services.AlmostCityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class AlmostCityControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AlmostCityService almostCityService;

    private List<AlmostCityEntity> exampleCities;

    void setUpWith10Cities() {
        AlmostCityEntity exampleCity = new AlmostCityEntity();
        exampleCity.setCity("Paris");
        exampleCity.setCapital(true);
        exampleCity.setLatitude(48.856613);
        exampleCity.setLongitude(2.352222);
        AlmostCityEntity exampleCity1 = new AlmostCityEntity();
        exampleCity1.setCity("Berlin");
        exampleCity1.setCapital(true);
        exampleCity.setLatitude(52.52437);
        exampleCity.setLongitude(13.41053);
        List<AlmostCityEntity> list = new ArrayList<>();
        for(int i = 0; i < 5; i++){
            exampleCity.setId((long) (i*2));
            exampleCity1.setId((long) (i*2+1));
            list.add(exampleCity);
            list.add(exampleCity1);
        }
        exampleCities = list;
    }

    void setUpWith2Cities() {
        AlmostCityEntity exampleCity = new AlmostCityEntity();
        exampleCity.setCity("Paris");
        exampleCity.setCapital(true);
        exampleCity.setId(1L);
        AlmostCityEntity exampleCity1 = new AlmostCityEntity();
        exampleCity1.setCity("Berlin");
        exampleCity1.setCapital(true);
        exampleCity1.setId(2L);
        exampleCities = Arrays.asList(
                exampleCity,exampleCity1
        );
    }

    @Test
    void loadAlmostCityEntitiesUnauthorized() throws Exception {
        setUpWith2Cities();
        when(almostCityService.loadSampleCities()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/cities"))
                .andExpect(status().isForbidden());
    }

    @Test
    void loadAlmostCityEntitiesExpected() throws Exception {
        setUpWith10Cities();
        when(almostCityService.loadSampleCities()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/cities")
                .with(user("user@user.at").password("user1234")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].city").value("Paris"));
    }

    @Test
    void loadAlmostCityEntitiesShort() throws Exception {
        setUpWith2Cities();
        when(almostCityService.loadSampleCities()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/cities")
                        .with(user("user@user.at").password("user1234")))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void loadAlmostCityCapitalEntitiesUnauthorized() throws Exception {
        setUpWith2Cities();
        when(almostCityService.loadSampleCapitals()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/capitals"))
                .andExpect(status().isForbidden());

    }
    @Test
    void loadAlmostCityCapitalEntitiesExpected() throws Exception {
        setUpWith10Cities();
        when(almostCityService.loadSampleCapitals()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/capitals")
                        .with(user("user@user.at").password("user1234")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].city").value("Paris"));

    }
    @Test
    void loadAlmostCityCapitalEntitiesShort() throws Exception {
        setUpWith2Cities();
        when(almostCityService.loadSampleCapitals()).thenReturn(exampleCities);
        mockMvc.perform(get("/api/almost/capitals")
                        .with(user("user@user.at").password("user1234")))
                .andExpect(status().isInternalServerError());
    }

}