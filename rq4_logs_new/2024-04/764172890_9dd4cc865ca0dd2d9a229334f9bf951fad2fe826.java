package com.aeon.hadog.base.code;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

@AllArgsConstructor
@Getter
public enum ErrorCode {
    /*
     * 400 BAD_REQUEST: 잘못된 요청
     */
    DUPLICATE_ID_REQUEST(HttpStatus.BAD_REQUEST, "중복된 아이디가 있습니다."),
    DUPLICATE_NICKNAME_REQUEST(HttpStatus.BAD_REQUEST, "중복된 닉네임이 있습니다."),
    DUPLICATE_EMAIL_REQUEST(HttpStatus.BAD_REQUEST, "중복된 이메일이 있습니다."),

    /*
     * 403 FORBIDDEN : 서버가 해당 요청 거부
     */
    INVALID_LOGIN(HttpStatus.FORBIDDEN, "아이디 또는 비밀번호가 잘못 입력되었습니다."),
    PASSWORD_MISMATCH(HttpStatus.FORBIDDEN, "비밀번호가 잘못 입력되었습니다."),

    /*
     * 404 NOT_FOUND : 잘못된 리소스 접근
     */
    USER_NOT_FOUND(HttpStatus.NOT_FOUND, "해당하는 유저가 존재하지 않습니다");


    /*
     * 500 INTERNAL SERVER ERROR : 서버 에러
     */

    private final HttpStatus status;
    private final String message;
}