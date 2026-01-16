package com.mydiary.my_diary_server.controller;


import com.mydiary.my_diary_server.service.DiaryService;
import com.mydiary.my_diary_server.dto.*;

import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/calendars")
public class CalendarController {
    private final DiaryService diaryService;
    
    @Operation (summary = "캘린더 정보를 받아오는 기능")
    @GetMapping("/date")
	public ResponseEntity<Map<String, String>> getCalender(Principal principal) {
		// 다이어리 정보 받아오기 (date, emotion)
		List<CalendarResponse> calendarResponses = diaryService.getCalendar(Long.parseLong(principal.getName()));
		// List<CalendarResponse>를 Map<Date, String>으로 직접 변환
		Map<String, String> response = new HashMap<>();
		for (CalendarResponse calendarResponse : calendarResponses) {
			response.put(calendarResponse.getFormattedDate(), calendarResponse.getEmotion());
		}

		return ResponseEntity.ok().body(response);

	}
	// 주석추가

	// 주석추가 2
}

