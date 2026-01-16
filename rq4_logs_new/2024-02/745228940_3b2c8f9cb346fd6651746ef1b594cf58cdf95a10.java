package org.example.JavaTaigaCode.controllers;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import org.example.JavaTaigaCode.models.TaskDTO;
import org.example.JavaTaigaCode.service.FoundWorkService;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@SpringBootTest
@AutoConfigureMockMvc
class FoundWorkControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private FoundWorkService foundWorkService;

    @InjectMocks
    private FoundWorkController foundWorkController;

    @Test
    void getFoundWorkByID_ShouldReturnFoundWorkList() throws Exception {
        // Arrange
        Integer milestoneID = 123;
        List<TaskDTO> mockTaskList = Arrays.asList(new TaskDTO(), new TaskDTO());

        // Mock the service behavior
        when(foundWorkService.getFoundWork(milestoneID)).thenReturn(mockTaskList);

        // Act and Assert
        mockMvc.perform(MockMvcRequestBuilders.get("/api/foundWork/{milestoneID}", milestoneID))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
                .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(mockTaskList.size()));
    }
}