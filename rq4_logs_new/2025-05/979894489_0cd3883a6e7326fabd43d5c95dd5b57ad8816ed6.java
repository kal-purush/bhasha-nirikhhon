package org.example.atharvolunteeringplatform.Controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.atharvolunteeringplatform.Api.ApiResponse;
import org.example.atharvolunteeringplatform.Model.Opportunity;
import org.example.atharvolunteeringplatform.Service.OpportunityService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/opportunity")
@RequiredArgsConstructor
public class OpportunityController {

    private final OpportunityService opportunityService;

    @GetMapping("/get-all")
    public ResponseEntity getAllOpportunities() {
        return ResponseEntity.status(HttpStatus.OK).body(opportunityService.findAllOpportunities());
    }

    @PostMapping("/add/{organizationId}")
    public ResponseEntity addOpportunity(
            @RequestBody @Valid Opportunity opportunity,
            /*@AuthenticationPrincipal*/ @PathVariable Integer organizationId) {

        opportunityService.createOpportunity(opportunity,organizationId);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Opportunity added successfully and pending approval"));
    }

    @PutMapping("/update/{opportunityId}/{organizationId}")
    public ResponseEntity updateOpportunity(
            /*@AuthenticationPrincipal*/ @PathVariable Integer opportunityId,
                                         @PathVariable Integer organizationId,
                                         @RequestBody @Valid Opportunity opportunity) {

        opportunityService.updateOpportunity(opportunityId, organizationId, opportunity);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Opportunity updated successfully"));
    }

    @DeleteMapping("/delete/{opportunityId}")
    public ResponseEntity deleteOpportunity(/*@AuthenticationPrincipal*/ @PathVariable Integer opportunityId) {
        opportunityService.deleteOpportunity(opportunityId);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Opportunity deleted successfully"));
    }
}