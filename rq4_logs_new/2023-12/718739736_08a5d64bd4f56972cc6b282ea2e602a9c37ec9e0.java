package com.example.planties.core.response;

public class BaseResultResponse<T> {
    private final StatusResult status;
    private final T data;
    private final String message;
    private final int code;

    public BaseResultResponse(StatusResult status, T data, String message, int code) {
        this.status = status;
        this.data = data;
        this.message = message;
        this.code = code;
    }

    public StatusResult getStatus() {
        return status;
    }

    public T getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }

    public int getCode() {
        return code;
    }

    public boolean isSuccess() {
        return status == StatusResult.SUCCESS;
    }

    public boolean isFailure() {
        return status == StatusResult.FAILURE;
    }

    public static <T> BaseResultResponse<T> success(T data, int code) {
        return new BaseResultResponse<>(StatusResult.SUCCESS, data, "success", code);
    }

    public static <T> BaseResultResponse<T> failure(String msg, T data, int code) {
        return new BaseResultResponse<>(StatusResult.FAILURE, data, msg, code);
    }
}