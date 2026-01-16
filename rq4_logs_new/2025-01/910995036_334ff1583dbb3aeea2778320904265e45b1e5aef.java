package com.example.lifeonhana.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.WalletDTO;
import com.example.lifeonhana.service.WalletService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/wallet")
@Tag(name="Wallet API", description = "하나지갑 관련 api")
public class WalletController {

	WalletService walletService;

	public WalletController(WalletService walletService) {
		this.walletService = walletService;
	}

	@PostMapping("")
	@Operation(summary = "하나지갑 정보 등록", description = "하나지갑 정보를 등록합니다.")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "등록 성공"),
		@ApiResponse(responseCode = "400", description = "잘못된 요청입니다."),
		@ApiResponse(responseCode = "401", description = "인증 실패"),
		@ApiResponse(responseCode = "404", description = "하나지갑 정보를 등록할 수 없습니다.")
	})
	@SecurityRequirement(name = "bearerAuth")
	public ResponseEntity<ApiResult> createWallet(@RequestHeader ("Authorization") String token,
		@RequestBody WalletDTO wallet) {
		WalletDTO walletDTO = walletService.creatWallet(wallet, token);
		return ResponseEntity.ok(new ApiResult(200, HttpStatus.OK, "하나지갑 정보 등록 성공", walletDTO));
	}

}