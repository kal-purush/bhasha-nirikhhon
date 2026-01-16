package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.ResponseResult;
import com.example.EnjoyTripBackend.dto.camping.CampingResponseDto;
import com.example.EnjoyTripBackend.service.CampingService;
import com.example.EnjoyTripBackend.util.argumentresolver.LimitedSizePagination;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CampingController {

    private final CampingService campingService;

    @GetMapping("/camping")
    @LimitedSizePagination(maxSize = 20)
    public ResponseEntity<ResponseResult<List<CampingResponseDto>>> findAll(@PageableDefault(size = 6) Pageable pageable) {
        return ResponseEntity.ok().body(campingService.findAll(pageable));
    }
}