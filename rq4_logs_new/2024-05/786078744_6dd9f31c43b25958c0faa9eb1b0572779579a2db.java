package com.ex.ticket.common.vo;

import com.ex.ticket.common.domain.ErrorReason;
import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Getter
public class ErrorResponse {

    private final boolean success = false;
    private final HttpStatus status;
    private final int code;
    private final String reason;
    private final LocalDateTime timeStamp;
    private final String path;

    public ErrorResponse(ErrorReason errorReason, String path) {
        this.status = errorReason.getHttpStatus();
        this.code = errorReason.getErrorCode();
        this.reason = errorReason.getErrorMessage();
        this.timeStamp = LocalDateTime.now();
        this.path = path;
    }

    public ErrorResponse(HttpStatus status, int code, String reason, String path) {
        this.status = status;
        this.code = code;
        this.reason = reason;
        this.timeStamp = LocalDateTime.now();
        this.path = path;
    }
}