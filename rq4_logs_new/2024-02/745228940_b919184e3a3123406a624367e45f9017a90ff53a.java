package org.example.JavaTaigaCode.controllers;

import org.example.JavaTaigaCode.models.AdoptedWorkDTO;
import org.example.JavaTaigaCode.service.AdoptedWorkService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class AdoptedWorkControllerTest {
    @Mock
    private AdoptedWorkService adoptedWorkService;
    @InjectMocks
    private AdoptedWorkController adoptedWorkController;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetUSAddedAfterSprintPlanning() {
        // Mock the behavior of the adoptedWorkService
        AdoptedWorkDTO expectedDTO = new AdoptedWorkDTO(1, 23, 123);
        when(adoptedWorkService.getUSAddedAfterSprintPlanning(1)).thenReturn(expectedDTO);

        // Call the controller method
        AdoptedWorkDTO responseEntity = adoptedWorkController.getUSAddedAfterSprintPlanning(1);

        // Verify that the service method was called with the correct parameter
        verify(adoptedWorkService).getUSAddedAfterSprintPlanning(1);

        // Assert the response entity
        assertEquals(responseEntity.getSprintTotalPoints(), 23);
        assertEquals(responseEntity.getAdoptedWork(),1);
    }

}
