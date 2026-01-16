package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.dto.ResponseResult;
import com.example.EnjoyTripBackend.dto.golf.GolfResponseDto;
import com.example.EnjoyTripBackend.service.GolfService;
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
public class GolfController {

    private final GolfService golfService;

    @GetMapping("/golf")
    public ResponseEntity<ResponseResult<List<GolfResponseDto>>> golfList(@PageableDefault(size = 20) Pageable pageable){
        return ResponseEntity.ok().body(golfService.golfList(pageable));
    }

}