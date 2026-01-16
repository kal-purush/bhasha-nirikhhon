package refooding.api.common.exception;

import org.springframework.validation.BindingResult;

import java.util.List;

public record FieldErrorResponse(
        String filed,
        Object rejectedValue,
        String message
) {
    public static List<FieldErrorResponse> of(BindingResult bindingResult) {
        return bindingResult
                .getFieldErrors()
                .stream()
                .map(fieldError -> new FieldErrorResponse(
                        fieldError.getField(),
                        fieldError.getRejectedValue(),
                        fieldError.getDefaultMessage()))
                .toList();
    }
}