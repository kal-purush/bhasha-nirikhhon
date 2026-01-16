package org.example.JavaTaigaCode.controllers;

import org.example.JavaTaigaCode.models.MilestoneDTO;
import org.example.JavaTaigaCode.service.DeliveryOnTimeService;
import org.example.JavaTaigaCode.service.ProjectService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class DoTControllerTest {
    @Mock
    private ProjectService projectService;

    @Mock
    private DeliveryOnTimeService deliveryOnTimeService;

    @InjectMocks
    private DoTController doTController;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(doTController).build();
    }

    @Test
    void getClosedMilestonesforBV() throws Exception {
        int projectID = 123;
        List<MilestoneDTO> milestones = Collections.singletonList(new MilestoneDTO());

        // Mock the behavior of deliveryOnTimeService.getClosedMilestonesbyID()
        when(deliveryOnTimeService.getClosedMilestonesbyID(projectID)).thenReturn(milestones);

        // Perform GET request and verify the response
        mockMvc.perform(MockMvcRequestBuilders.get("/api/DoT/{projectID}/BV", projectID))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(milestones.size()));
    }
}