package com.example.auction.Global.error.errorcode;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import static org.springframework.http.HttpStatus.*;

@Getter
@AllArgsConstructor
public enum ErrorCode {
    /* 400 BAD_REQUEST : 잘못된 요청 */
    BAD_REQUEST_RESOURCE(BAD_REQUEST, "잘못된 리소스 값을 입력했습니다."),

    /* 401 UNAUTHORIZED : 인증되지 않은 사용자 */

    /* 403 FORBIDDEN : 권한이 없음 */
    FORBIDDEN_ERROR(FORBIDDEN,"권한이 없습니다."),

    /* 404 NOT_FOUND : Resource 를 찾을 수 없음 */
    RESOURCES_NOT_FOUND(NOT_FOUND, "해당 리소트 값을 찾을 수 없습니다."),


    /* 409 CONFLICT : Resource 의 현재 상태와 충돌. 보통 중복된 데이터 존재 */
    DUPLICATE_RESOURCE(CONFLICT, "데이터가 이미 존재합니다"),
    ;

    private final HttpStatus httpStatus;
    private final String detail;
}