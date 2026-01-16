package com.example.musing.common.dto;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ResponseDto<T> { //반환할 데이터와 메세지를 같이 구분해서 보내기 위해 사용
    private final T data;
    private final String message;

    @Builder(access = AccessLevel.PRIVATE)
    private ResponseDto(T data, String message) {
        this.data = data;
        this.message = message;
    }

    public static <T> ResponseDto<T> of(T data, String message) {
        return ResponseDto.<T>builder()
                .data(data)
                .message(message)
                .build();
    }

    public static <T> ResponseDto<T> of(T data) {
        return ResponseDto.of(data, "");
    }
}