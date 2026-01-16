package com.example.lifeonhana.global.exception;

import org.springframework.http.HttpStatus;

import lombok.Getter;

@Getter
public enum ErrorCode {
    // 인증 관련 에러
    UNAUTHORIZED_ACCESS(HttpStatus.UNAUTHORIZED, "A001", "인증되지 않은 접근입니다."),
    
    // 계좌 관련 에러
    ACCOUNT_NOT_FOUND(HttpStatus.NOT_FOUND, "AC001", "계좌를 찾을 수 없습니다."),
    INSUFFICIENT_BALANCE(HttpStatus.BAD_REQUEST, "AC002", "잔액이 부족합니다."),
    SAME_ACCOUNT_TRANSFER(HttpStatus.BAD_REQUEST, "AC003", "출금 계좌와 입금 계좌가 동일할 수 없습니다."),
    NEGATIVE_TRANSFER_AMOUNT(HttpStatus.BAD_REQUEST, "AC004", "이체 금액은 0보다 커야 합니다."),
    
    // 사용자 관련 에러
    USER_NOT_FOUND(HttpStatus.NOT_FOUND, "U001", "사용자를 찾을 수 없습니다.");

    private final HttpStatus httpStatus;
    private final String code;
    private final String message;

    ErrorCode(HttpStatus httpStatus, String code, String message) {
        this.httpStatus = httpStatus;
        this.code = code;
        this.message = message;
    }
} 