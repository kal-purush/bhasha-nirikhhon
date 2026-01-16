package br.com.mercado.domain.dto.error;

import br.com.mercado.domain.enums.ErrorCode;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ErrorResponse {
    private ErrorCode errorCode;
    private String message;

}