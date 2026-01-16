package org.example.atharvolunteeringplatform.Controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.atharvolunteeringplatform.Api.ApiResponse;
import org.example.atharvolunteeringplatform.Model.Badge;
import org.example.atharvolunteeringplatform.Service.BadgeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/api/v1/badge")
@RequiredArgsConstructor

public class BadgeController {
    private final BadgeService badgeService;

    @PostMapping("/add")
    public ResponseEntity<?> addBadge(@RequestPart @Valid Badge badge, @RequestPart MultipartFile image) {
        badgeService.createBadge(badge, image);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Badge created successfully"));
    }

    @PutMapping("/update/{id}")
    public ResponseEntity<?> updateBadge(@PathVariable Integer id, @RequestPart @Valid Badge badge, @RequestPart(required = false) MultipartFile image) {
        badgeService.updateBadge(id, badge, image);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Badge updated successfully"));
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteBadge(@PathVariable Integer id) {
        badgeService.deleteBadge(id);
        return ResponseEntity.status(HttpStatus.OK).body(new ApiResponse("Badge deleted successfully"));
    }



}