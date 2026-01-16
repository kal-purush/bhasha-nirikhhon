package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.NonPagingResponseResult;
import com.example.EnjoyTripBackend.dto.ResponseResult;
import com.example.EnjoyTripBackend.dto.house.HouseRequestDto;
import com.example.EnjoyTripBackend.dto.house.HouseResponseDto;
import com.example.EnjoyTripBackend.service.HouseService;
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
public class HouseController {

    private final HouseService houseService;

    @GetMapping("/houses")
    public ResponseEntity<ResponseResult<List<HouseResponseDto>>> houseList(@PageableDefault(size = 20) Pageable pageable){
        return ResponseEntity.ok().body(houseService.houseList(pageable));
    }

    @GetMapping("/houses/{id}")
    public ResponseEntity<NonPagingResponseResult<HouseResponseDto>> houseDetail(@PathVariable("id")String id){
        return ResponseEntity.ok().body(houseService.findById(id));
    }

    @PostMapping("/houses/search")
    @LimitedSizePagination(maxSize = 20)
    public ResponseEntity<ResponseResult<List<HouseResponseDto>>> houseSearchList(@PageableDefault(size = 20) Pageable pageable, @RequestBody HouseRequestDto requestDto){
        return ResponseEntity.ok().body(houseService.houseSearchList(pageable, requestDto));
    }
}