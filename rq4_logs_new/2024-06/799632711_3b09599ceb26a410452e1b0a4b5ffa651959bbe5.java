package com.LuuQu.medicalclinic.controller;

import com.LuuQu.medicalclinic.model.dto.FacilityDto;
import com.LuuQu.medicalclinic.service.FacilityService;
import com.LuuQu.medicalclinic.testHelper.TestControllerHelper;
import com.LuuQu.medicalclinic.testHelper.TestData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class FacilityControllerTest {
    @MockBean
    FacilityService facilityService;
    @Autowired
    MockMvc mockMvc;
    @Autowired
    ObjectMapper objectMapper;

    @Test
    void getFacilities_DataCorrect_DtoListReturned() throws Exception {
        List<FacilityDto> list = TestData.FacilityDtoFactory.getList(10);
        String expectedResult = TestControllerHelper.getObjectAsString(list, objectMapper);
        when(facilityService.getFacilities(PageRequest.of(0, 10))).thenReturn(list);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.get("/facilities")
                        .param("page", "0")
                        .param("size", "10"))
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(expectedResult, result.getResponse().getContentAsString());
    }

    @Test
    void getFacility_DataCorrect_DtoReturned() throws Exception {
        FacilityDto facilityDto = TestData.FacilityDtoFactory.get(1L);
        String expectedResult = TestControllerHelper.getObjectAsString(facilityDto, objectMapper);
        when(facilityService.getFacility(1L)).thenReturn(facilityDto);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.get("/facilities/{id}", 1L))
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(expectedResult, result.getResponse().getContentAsString());
    }

    @Test
    void addFacility_DataCorrect_DtoReturned() throws Exception {
        FacilityDto facilityDto = TestData.FacilityDtoFactory.get(1L);
        String request = TestControllerHelper.getObjectAsString(TestData.FacilityDtoFactory.get(), objectMapper);
        String expectedResult = TestControllerHelper.getObjectAsString(facilityDto, objectMapper);
        when(facilityService.addFacility(TestData.FacilityDtoFactory.get())).thenReturn(facilityDto);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.post("/facilities")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andDo(print())
                .andExpect(status().isCreated())
                .andReturn();

        Assertions.assertEquals(expectedResult, result.getResponse().getContentAsString());
    }

    @Test
    void updateFacility_DataCorrect_DtoReturned() throws Exception {
        FacilityDto facilityDto = TestData.FacilityDtoFactory.get(1L);
        String request = TestControllerHelper.getObjectAsString(TestData.FacilityDtoFactory.get(), objectMapper);
        String expectedResult = TestControllerHelper.getObjectAsString(facilityDto, objectMapper);
        when(facilityService.editFacility(1L, TestData.FacilityDtoFactory.get())).thenReturn(facilityDto);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.patch("/facilities/{id}", 1L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(expectedResult, result.getResponse().getContentAsString());
    }

    @Test
    void deleteFacility_DataCorrect_DtoReturned() throws Exception {
        this.mockMvc
                .perform(MockMvcRequestBuilders.delete("/facilities/{id}", 1L))
                .andDo(print())
                .andExpect(status().isNoContent());

        verify(facilityService, times(1)).deleteFacility(1L);
    }
}