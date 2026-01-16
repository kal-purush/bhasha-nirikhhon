package com.spartanullnull.otil.global.exception;

import com.fasterxml.jackson.annotation.*;
import java.time.*;
import java.util.*;
import lombok.*;
import org.springframework.validation.*;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public class ErrorDetail {

    private String field;

    private String value;

    private String reason;

    private LocalDateTime timestamp;

    public ErrorDetail(String field, String value, LocalDateTime timestamp) {
        this.field = field;
        this.value = value;
        this.timestamp = timestamp;
    }

    public ErrorDetail(String field, String value, String reason, LocalDateTime timestamp) {
        this.field = field;
        this.value = value;
        this.reason = reason;
        this.timestamp = timestamp;
    }

    /**
     * 이 메서드의 역할
     *
     * @param bindingResult 파라미터의 역할
     * @return 반환값은 어떤 반환값인지 설명
     */
    public static List<ErrorDetail> of(BindingResult bindingResult) {
        List<FieldError> fieldErrors = bindingResult.getFieldErrors();
        return fieldErrors.stream()
            .map(fieldError ->
                new ErrorDetail(
                    fieldError.getField(),
                    Objects.requireNonNull(fieldError.getRejectedValue()).toString(),
                    fieldError.getDefaultMessage(),
                    LocalDateTime.now()
                )
            )
            .toList();
    }
}