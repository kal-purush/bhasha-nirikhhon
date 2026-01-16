package com.bodytok.healthdiary.domain.constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public enum ErrorMessage {
    WRONG_TYPE_TOKEN(400, "잘못된 토큰 타입입니다."),
    UNSUPPORTED_TOKEN(400, "지원되지 않는 토큰입니다."),
    EXPIRED_TOKEN(401, "만료된 토큰입니다."),
    UNKNOWN_ERROR(500, "알 수 없는 오류가 발생했습니다."),
    ACCESS_DENIED(403, "접근 불가한 요청입니다.");

    private final int code;
    private final String msg;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    ErrorMessage(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String toJsonString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "";
        }
    }
}