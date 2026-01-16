package com.example.backend.domain.festival.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.example.backend.domain.festival.dto.response.FestivalDetailResponse;
import com.example.backend.domain.festival.service.FestivalService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class FestivalController {
	private final FestivalService festivalService;

	@GetMapping("/festivals/{id}")
	public ResponseEntity<FestivalDetailResponse> getFestivalDetail(@PathVariable Long id) {
		FestivalDetailResponse response = festivalService.findDetailFestival(id);
		return ResponseEntity.ok(response);
	}


}