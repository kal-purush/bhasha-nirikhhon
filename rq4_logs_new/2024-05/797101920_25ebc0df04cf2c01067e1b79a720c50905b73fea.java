package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.NonPagingResponseResult;
import com.example.EnjoyTripBackend.dto.ResponseResult;
import com.example.EnjoyTripBackend.dto.airplane.AirplaneRequestDto;
import com.example.EnjoyTripBackend.dto.airplane.AirplaneResponseDto;
import com.example.EnjoyTripBackend.service.AirplaneService;
import com.example.EnjoyTripBackend.util.LimitedSizePagination;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AirplaneController {

    private final AirplaneService airplaneService;

    @GetMapping("/airplanes")
    public ResponseEntity<ResponseResult<List<AirplaneResponseDto>>> airplaneList(@PageableDefault(size = 20) Pageable pageable){
        return ResponseEntity.ok().body(airplaneService.airplaneList(pageable));
    }

    @GetMapping("/airplanes/{id}")
    public ResponseEntity<NonPagingResponseResult<AirplaneResponseDto>> airplaneDetail(@PathVariable("id")Long id){
        return ResponseEntity.ok().body(airplaneService.findById(id));
    }

    @PostMapping("/airplanes/search")
    @LimitedSizePagination(maxSize = 20)
    public ResponseEntity<ResponseResult<List<AirplaneResponseDto>>> airplaneSearchList(@PageableDefault(size = 20) Pageable pageable, @RequestBody AirplaneRequestDto requestDto){
        return ResponseEntity.ok().body(airplaneService.airplaneSearchList(pageable, requestDto));
    }
}