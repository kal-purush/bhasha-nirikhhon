package com.golfzon.social.meeting.controller;

import com.golfzon.social.meeting.service.MeetingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/meeting")
public class MeetingController {

    private final MeetingService companyService;

    //
    @PostMapping(value = "/post")
    public ResponseEntity<String> meetingPost() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        log.info("principal:{}",principal);
        //Member member = ((UserDetailsImpl)principal).getMember();

        return ResponseEntity.ok()
                .contentType(new MediaType("application", "json", StandardCharsets.UTF_8))
                .body("result : ");
    }

    //
    @GetMapping(value = "/")
    public ResponseEntity<String> companySelectAll() {

        // 업체 신청 목록보기
        return ResponseEntity.ok()
                .body("result : ");
    }
}