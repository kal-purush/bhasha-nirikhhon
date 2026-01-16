package com.fatepet.global.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponse<T> {

    private int status;

    private String message;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private T data;

    private ApiResponse(ResponseCode code, T data) {
        this.status = code.getStatusCode();
        this.message = code.getMessage();
        this.data = data;
    }

    public static <T> ApiResponse<T> of(ResponseCode code, T data) {
        return new ApiResponse<>(code, data);
    }

    public static ApiResponse<Void> of(ResponseCode code) {
        return new ApiResponse<>(code, null);
    }

    public static <T> ApiResponse<T> of(int status, String message, T data) {
        return new ApiResponse<>(status, message, data);
    }

    public static ApiResponse<Void> of(int status, String message) {
        return new ApiResponse<>(status, message, null);
    }
}