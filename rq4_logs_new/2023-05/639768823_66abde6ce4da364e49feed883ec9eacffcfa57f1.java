package com.dateplan.dateplan.domain.member.controller;

import com.dateplan.dateplan.domain.member.dto.PhoneAuthCodeRequest;
import com.dateplan.dateplan.domain.member.dto.PhoneRequest;
import com.dateplan.dateplan.domain.member.service.AuthService;
import com.dateplan.dateplan.global.dto.response.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/auth")
public class AuthController {

	private final AuthService authService;

	@PostMapping("/phone")
	public ApiResponse<Void> sendCode(@RequestBody @Valid PhoneRequest request) {

		authService.sendSms(request.toServiceRequest());

		return ApiResponse.ofSuccess();
	}

	@PostMapping("/phone/code")
	public ApiResponse<Void> authenticateCode(@RequestBody @Valid PhoneAuthCodeRequest request) {

		authService.authenticateAuthCode(request.toServiceRequest());

		return ApiResponse.ofSuccess();
	}
}