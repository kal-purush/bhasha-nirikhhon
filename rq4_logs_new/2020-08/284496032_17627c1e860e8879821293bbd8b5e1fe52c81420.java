package com.kafkaExample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaExample.Producer.EmpEventProducer;
import com.kafkaExample.controller.Employee;
import com.kafkaExample.model.EmployeeEvent;
import com.kafkaExample.model.EmployeeModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(Employee.class)  // Advantage of webmvctest is that scanning is done only for controller layer
@AutoConfigureMockMvc // to write unit test for endpoints
public class EmployeeEventUnitTest {

    @Autowired
    MockMvc mockMvc; // Will have access to all endpoints written in controller or simulate the behaviour of endpoints

    @MockBean // Injects the external dependencies mentioned in controller
            EmpEventProducer empEventProducer;

    ObjectMapper objectMapper = new ObjectMapper(); // Convert employee event to json

    @Test
    void postEmployeeEvent() throws Exception {

        EmployeeModel employeeModel = EmployeeModel.builder().empId(1).empName("Keerthana").empGender("Female").build();
        EmployeeEvent employeeEvent = EmployeeEvent.builder().empEventId(null).employeeModel(employeeModel).build();

        String json = objectMapper.writeValueAsString(employeeEvent);

        doNothing().when(empEventProducer).sendEmployeeEvent(isA(EmployeeEvent.class)); // Mocking for void method of sendEmployeeEvent

        mockMvc.perform(post("/kafka/createEmployee").
                content(json).contentType(MediaType.APPLICATION_JSON)).
                andExpect(status().isCreated()); // asserting the status returned from endpoint

    }

}