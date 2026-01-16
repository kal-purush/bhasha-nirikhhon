package com.example.demo.exception;

import org.springframework.http.HttpStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ErrorCode {

    INVALID_TOKEN(HttpStatus.UNAUTHORIZED, "토큰이 유효하지 않습니다."),
    
    USER_NOT_FOUND(HttpStatus.NOT_FOUND, "사용자를 찾을 수 없습니다."),
    
    DUPLICATE_USERNAME(HttpStatus.CONFLICT, "이미 존재하는 사용자입니다."),

    VALIDATION_ERROR(HttpStatus.BAD_REQUEST, "입력값이 유효하지 않습니다."),
    
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "서버 오류가 발생했습니다."),

    EXPIRED_TOKEN(HttpStatus.UNAUTHORIZED, "토큰이 만료되었습니다."),

    INVALID_CREDENTIALS(HttpStatus.UNAUTHORIZED, "비밀번호가 올바르지 않습니다."),

    MATCHING_TIMEOUT(HttpStatus.REQUEST_TIMEOUT, "매칭 대기시간이 초과되었습니다."),

    CHAT_ROOM_NOT_FOUND(HttpStatus.NOT_FOUND, "채팅방이 존재하지 않습니다."),

    MESSAGE_SEND_FAILED(HttpStatus.INTERNAL_SERVER_ERROR, "메시지 전송이 실패했습니다.");


    private final HttpStatus status;
    private final String message;
}