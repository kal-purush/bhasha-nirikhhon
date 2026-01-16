package com.example.EnjoyTripBackend.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class EnjoyTripException extends RuntimeException {

    private ErrorCode errorCode;
    private String message;

    public EnjoyTripException(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String getMessage() {
        if (message == null) {
            message = errorCode.getMessage();
        }
        return message;
    }
}