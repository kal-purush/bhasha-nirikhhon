package com.spartanullnull.otil.global.exception;

import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.springframework.http.*;

@Getter
@AllArgsConstructor
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public class ErrorCode {

    private String message;

    private int code;

    private HttpStatus status;

    public static ErrorCode of(ErrorCase errorCase) {
        return new ErrorCode(
            ErrorCaseResolver.getMsg(errorCase),
            ErrorCaseResolver.getCode(errorCase),
            ErrorCaseResolver.getStatus(errorCase)
        );
    }
}