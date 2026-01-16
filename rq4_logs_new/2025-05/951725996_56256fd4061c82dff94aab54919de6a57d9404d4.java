package com.mallang.mallnagorder.ai.controller;

import com.mallang.mallnagorder.ai.service.AdminPayloadForwardService;
import com.mallang.mallnagorder.ai.service.AdminPayloadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/ai")
@Slf4j
public class AdminPayloadController {

    private final AdminPayloadService adminPayloadService;
    private final AdminPayloadForwardService forwardService;

    @PostMapping("/trigger")
    public void triggerAllAdminPayloads() {
        adminPayloadService.generateAllPayloadsAndForward();
    }
}