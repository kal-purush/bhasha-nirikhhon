package com.roomfit.be.global.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
@AllArgsConstructor
public enum ErrorCode {
    // 공통 에러
    COMMON_BAD_REQUEST(HttpStatus.BAD_REQUEST, "잘못된 요청입니다."),
    COMMON_NOT_FOUND(HttpStatus.NOT_FOUND, "리소스를 찾을 수 없습니다."),
    COMMON_INTERNAL_SERVER_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "서버 내부 오류가 발생했습니다."),

    // 유저 관련 에러
    USER_NOT_FOUND(HttpStatus.NOT_FOUND, "유저를 찾을 수 없습니다."),
    USER_EMAIL_VERIFICATION_REQUIRED(HttpStatus.BAD_REQUEST, "이메일 인증이 필요합니다."),

    // 인증 관련 에러
    AUTH_INVALID_ID_OR_PASSWORD(HttpStatus.UNAUTHORIZED, "아이디 또는 비밀번호가 일치하지 않습니다."),
    AUTH_VERIFICATION_CODE_EXPIRED(HttpStatus.BAD_REQUEST, "인증 코드가 만료되었습니다."),
    AUTH_VERIFICATION_CODE_NOT_FOUND(HttpStatus.BAD_REQUEST, "인증 코드가 일치하지 않습니다."),
    AUTH_TOKEN_GENERATION_FAILURE(HttpStatus.BAD_REQUEST, "토큰 발급에 실패하였습니다."),

    // 이메일 관련 에러
    EMAIL_SEND_FAILURE(HttpStatus.INTERNAL_SERVER_ERROR, "이메일 전송에 실패했습니다."),

    // 설문지 관련 에러
    QUESTIONNAIRE_NOT_FOUND(HttpStatus.NOT_FOUND, "설문지를 찾을 수 없습니다."),
    QUESTIONNAIRE_SAVE_FAILURE(HttpStatus.BAD_REQUEST, "설문 생성을 실패하였습니다."),
    REPLY_NOT_FOUND(HttpStatus.NOT_FOUND, "응답을 찾을 수 없습니다."),
    REPLY_SAVE_FAILURE(HttpStatus.BAD_REQUEST, "설문 응답 생성을 실패하였습니다."),

    // 채팅 관련 에러
    CHATROOM_NOT_FOUND(HttpStatus.NOT_FOUND, "채팅방을 찾을 수 없습니다.");

    private final HttpStatus status;
    private final String message;
}