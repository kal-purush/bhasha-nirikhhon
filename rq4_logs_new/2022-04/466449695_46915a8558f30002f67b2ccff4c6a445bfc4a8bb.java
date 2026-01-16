package com.mikaelsonbraz.parkingapi.api.parkingSpace.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mikaelsonbraz.parkingapi.api.customer.dto.CustomerDTO;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.dto.ParkingSpaceDTO;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.entity.ParkingSpace;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.service.ParkingSpaceService;
import com.mikaelsonbraz.parkingapi.api.renting.dto.RentingDTO;
import com.mikaelsonbraz.parkingapi.api.renting.model.entity.Renting;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.LocalDateTime;
import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@WebMvcTest(controllers = ParkingSpaceController.class)
@AutoConfigureMockMvc
public class ParkingSpaceControllerTest {

    static String SPACE_API = "/api/spaces";

    @Autowired
    MockMvc mvc;

    @MockBean
    ParkingSpaceService service;

    ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public ParkingSpaceDTO createNewSpaceDTO(){
        return ParkingSpaceDTO.builder().idSpace(1).occupied(false).spaceType(1).build();
    }

    public ParkingSpace createNewSpace(){
        return ParkingSpace.builder().idSpace(1).occupied(false).spaceType(1).build();
    }

    @Test
    @DisplayName("Must create a Parking Space")
    public void createParkingSpaceTest() throws Exception{
        //cenário
        ParkingSpaceDTO spaceDTO = createNewSpaceDTO();
        ParkingSpace savedSpace = createNewSpace();
        BDDMockito.given(service.save(Mockito.any(ParkingSpace.class))).willReturn(savedSpace);
        String json = mapper.writeValueAsString(spaceDTO);

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .post(SPACE_API)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(json);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.jsonPath("idSpace").isNotEmpty())
                .andExpect(MockMvcResultMatchers.jsonPath("occupied").value(savedSpace.isOccupied()))
                .andExpect(MockMvcResultMatchers.jsonPath("spaceType").value(savedSpace.getSpaceType().toString()));
    }

    @Test
    @DisplayName("Must return ParkingSpace details")
    public void getParkingSpaceDetails() throws Exception{
        //cenario
        ParkingSpace space = createNewSpace();
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.of(space));

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .get(SPACE_API.concat("/1"))
                .accept(MediaType.APPLICATION_JSON);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("idSpace").value(space.getIdSpace()))
                .andExpect(MockMvcResultMatchers.jsonPath("occupied").value(space.isOccupied()))
                .andExpect(MockMvcResultMatchers.jsonPath("spaceType").value(space.getSpaceType().toString()));
    }

    @Test
    @DisplayName("Must return Http Status Not FOund when not found a parking space by id")
    public void parkingSpaceNotFoundTest() throws Exception{
        //cenário
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.empty());

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .get(SPACE_API.concat("/1"))
                .accept(MediaType.APPLICATION_JSON);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

    @Test
    @DisplayName("Must delete a Parking Space")
    public void deleteParkingSpaceTest() throws Exception{
        //cenário
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.of(createNewSpace()));

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .delete(SPACE_API.concat("/1"));

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    @Test
    @DisplayName("Must return Http Status Not Found when trying delete not found parking space by id")
    public void deleteNonexistentParkingSPaceTest() throws Exception{
        //cenário
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.empty());

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .delete(SPACE_API.concat("/1"));

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

}