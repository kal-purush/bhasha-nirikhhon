package com.LuuQu.medicalclinic.controller;

import com.LuuQu.medicalclinic.testHelper.TestControllerHelper;
import com.LuuQu.medicalclinic.testHelper.TestData;
import com.LuuQu.medicalclinic.model.dto.AppointmentDto;
import com.LuuQu.medicalclinic.service.AppointmentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class AppointmentControllerTest {
    @MockBean
    AppointmentService appointmentService;
    @Autowired
    MockMvc mockMvc;
    @Autowired
    ObjectMapper objectMapper;

    @Test
    void addAppointment_DataCorrect_DtoReturned() throws Exception {
        AppointmentDto appointmentDtoInput = TestData.AppointmentDtoFactory.get();
        Long doctorId = 1L;
        AppointmentDto appointmentDtoOutput = TestData.AppointmentDtoFactory.get(1L);
        appointmentDtoOutput.setDoctor(TestData.DoctorSimpleDtoFactory.get(1L));
        when(appointmentService.addAppointment(doctorId, appointmentDtoInput)).thenReturn(appointmentDtoOutput);
        String requestJson = TestControllerHelper.getObjectAsString(appointmentDtoInput, objectMapper);
        String expectedResult = TestControllerHelper.getObjectAsString(appointmentDtoOutput, objectMapper);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.post("/appointments/{doctorId}", doctorId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(expectedResult, result.getResponse().getContentAsString());
    }

    @Test
    void addPatient_DataCorrect_DtoReturned() throws Exception {
        AppointmentDto appointmentDto = TestData.AppointmentDtoFactory.get(1L);
        appointmentDto.setDoctor(TestData.DoctorSimpleDtoFactory.get(1L));
        appointmentDto.setPatient(TestData.PatientDtoFactory.get(1L));
        when(appointmentService.addPatient(1L, 1L)).thenReturn(appointmentDto);
        String expectedResult = TestControllerHelper.getObjectAsString(appointmentDto, objectMapper);

        MvcResult result = this.mockMvc
                .perform(MockMvcRequestBuilders.patch("/appointments/{appointmentId}/patients/{patientId}", 1L, 1L))
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(removeEmptyLines(expectedResult), result.getResponse().getContentAsString());
    }

    String removeEmptyLines(String input) {
        return Arrays.stream(input.split("\r\n"))
                .filter(line -> !line.trim().isEmpty())
                .collect(Collectors.joining(""))
                .replaceAll(" ", "");
    }
}