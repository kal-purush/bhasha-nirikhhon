package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.dto.JobDTO;
import com.BackendChallenge.TechTrendEmporium.dto.ReviewJobRequestDTO;
import com.BackendChallenge.TechTrendEmporium.service.AdminReviewService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdminReviewControllerTest {

    @InjectMocks
    private AdminReviewController adminReviewController;

    @Mock
    private AdminReviewService adminReviewService;

    @Test
    void testGetPendingApprovalJobs() {
        // Mock pending approval jobs
        List<JobDTO> pendingJobs = new ArrayList<>();
        pendingJobs.add(new JobDTO(/* job details */));
        // Mock the behavior of adminReviewService.getPendingApprovalJobs()
        when(adminReviewService.getPendingApprovalJobs()).thenReturn(pendingJobs);

        // Call the method under test
        ResponseEntity<Map<String, List<JobDTO>>> responseEntity = adminReviewController.getPendingApprovalJobs();

        // Verify that adminReviewService.getPendingApprovalJobs() is called
        verify(adminReviewService, times(1)).getPendingApprovalJobs();

        // Assert the response status and body
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertNotNull(responseEntity.getBody());
        assertEquals(pendingJobs, responseEntity.getBody().get("jobs"));
    }
    @Test
    void testReviewJob_SuccessApproval() {
        // Mock request DTO
        ReviewJobRequestDTO requestDTO = new ReviewJobRequestDTO(/* Details of the Review*/);

        // Call the method under test
        ResponseEntity<Map<String, String>> responseEntity = adminReviewController.reviewJob("type", requestDTO);

        // Verify that adminReviewService.reviewJob is called with correct parameters
        verify(adminReviewService, times(1)).reviewJob(eq("type"), eq(requestDTO));

        // Assert the response status and body
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertNotNull(responseEntity.getBody());
        assertEquals("Success Approval", responseEntity.getBody().get("message"));
    }
    @Test
    void testReviewJob_MissingRequestBody() {
        // Define test parameters
        String validType = "validType";
        // Here, requestDTO is not provided intentionally to simulate a missing request body

        // Call the method under test
        ResponseEntity<Map<String, String>> responseEntity = adminReviewController.reviewJob(validType, null);

        // Assert the response status
        assertEquals(HttpStatus.BAD_REQUEST, responseEntity.getStatusCode());
        // Add assertions for response body if needed
    }




}
