package com.polzzak.domain.notification.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.polzzak.domain.notification.dto.NotificationResponse;
import com.polzzak.domain.notification.service.NotificationService;
import com.polzzak.global.common.ApiResponse;
import com.polzzak.global.security.LoginId;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/notifications")
public class NotificationController {

	private final NotificationService notificationService;

	private static final int NOTIFICATION_PAGE_SIZE = 10;
	private final String longMaxValue = "9223372036854775807";

	@GetMapping
	public ResponseEntity<ApiResponse<NotificationResponse>> getNotifications(final @LoginId Long memberId,
		@RequestParam(required = false, defaultValue = longMaxValue) final long startId) {
		return ResponseEntity.ok(
			ApiResponse.ok(notificationService.getNotificationResponse(memberId, NOTIFICATION_PAGE_SIZE, startId)));
	}
}