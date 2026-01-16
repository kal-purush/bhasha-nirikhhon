package com.roomfit.be.global.response;

public class ResponseFactory {

    // 성공 응답 (데이터 포함 + 커스텀 코드)
    public static <T> CommonResponse<T> success(T data, String message) {
        return CommonResponse.<T>builder()
                .success(true)
                .message(message)
                .data(data)
                .build();
    }

    public static <T> CommonResponse<T> success(String message) {
        return success(null, message );
    }
    // 실패 응답
    public static CommonResponse<Void> failure(String message) {
        return CommonResponse.<Void>builder()
                .success(false)
                .message(message)
                .build();
    }
}