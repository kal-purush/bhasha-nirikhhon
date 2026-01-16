package org.petmarket.users.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.petmarket.users.dto.ComplaintResponseDto;
import org.petmarket.users.entity.ComplaintStatus;
import org.petmarket.users.service.ComplaintService;
import org.petmarket.utils.annotations.parametrs.ParameterPageNumber;
import org.petmarket.utils.annotations.parametrs.ParameterPageSize;
import org.petmarket.utils.annotations.responses.ApiResponseForbidden;
import org.petmarket.utils.annotations.responses.ApiResponseNotFound;
import org.petmarket.utils.annotations.responses.ApiResponseSuccessful;
import org.petmarket.utils.annotations.responses.ApiResponseUnauthorized;
import org.springframework.data.domain.Sort;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "Complaints", description = "the user complaints API")
@Slf4j
@RequiredArgsConstructor
@RestController
@Validated
@RequestMapping(value = "/v1/admin/complaints")
public class ComplaintsAdminController {
    private final ComplaintService complaintService;

    @Operation(summary = "Delete complaint")
    @ApiResponseSuccessful
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @PreAuthorize("isAuthenticated()")
    @DeleteMapping("/{id}")
    public void deleteComplaint(@PathVariable Long id) {
        log.info("Deleting complaint");
        complaintService.deleteComplaint(id);
    }

    @Operation(summary = "Get complaint")
    @ApiResponseSuccessful
    @ApiResponseNotFound
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @PreAuthorize("isAuthenticated()")
    @GetMapping("/{id}")
    public ComplaintResponseDto getComplaint(@PathVariable Long id) {
        log.info("Getting complaint");
        return complaintService.getComplaint(id);
    }

    @Operation(summary = "Get complaints")
    @ApiResponseSuccessful
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @PreAuthorize("isAuthenticated()")
    @GetMapping
    public List<ComplaintResponseDto> getComplaints(
            @RequestParam(required = false, defaultValue = "PENDING") ComplaintStatus complaintStatus,
            @ParameterPageNumber @RequestParam(defaultValue = "1") @Positive int page,
            @ParameterPageSize @RequestParam(defaultValue = "30") @Positive int size,
            @RequestParam(defaultValue = "DESC") Sort.Direction direction) {
        log.info("Getting complaints");
        return complaintService.getComplaints(complaintStatus, size, page, direction);
    }

    @Operation(summary = "Get complaints by user id")
    @ApiResponseSuccessful
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @PreAuthorize("isAuthenticated()")
    @GetMapping("/user/{userId}")
    public List<ComplaintResponseDto> getComplaintsByUserId(
            @PathVariable Long userId,
            @RequestParam(required = false, defaultValue = "PENDING") ComplaintStatus status,
            @ParameterPageNumber @RequestParam(defaultValue = "1") @Positive int page,
            @ParameterPageSize @RequestParam(defaultValue = "30") @Positive int size,
            @RequestParam(defaultValue = "DESC") Sort.Direction direction) {
        log.info("Getting complaints by user id");
        return complaintService.getComplaintsByUserId(userId, status, size, page, direction);
    }

    @Operation(summary = "Update complaint status")
    @ApiResponseSuccessful
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @PreAuthorize("isAuthenticated()")
    @PutMapping("/{id}")
    public void updateComplaintStatus(@PathVariable Long id,
                                      @RequestParam(defaultValue = "RESOLVED") ComplaintStatus status) {
        log.info("Updating complaint status");
        complaintService.updateStatusById(id, status);
    }
}