package org.detective.controller.Estimate;

import org.detective.dto.AssignedRequestDTO;
import org.detective.entity.AssignmentRequest;
import org.detective.entity.Request;
import org.detective.services.Estimate.EstimateService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Objects;

@RestController
@RequestMapping("/estimate")
public class EstimateController {

    private EstimateService estimateService;

    public EstimateController(EstimateService estimateService) {
        this.estimateService = estimateService;
    }



    @GetMapping("")
    public List<AssignedRequestDTO> getAssingedRequest(@RequestParam(value = "email",required = true) String email) {
        System.err.println("Estimate Controller");
        List<AssignedRequestDTO> assignedRequest = estimateService.getAssignedRequest(email);
        System.out.println("Estimate Controller : " + assignedRequest.toString());
        System.out.println("Estimate Controller : " + assignedRequest.size());
        return assignedRequest;
    }
}