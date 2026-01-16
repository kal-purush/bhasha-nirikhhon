package com.spartanullnull.otil.global.exception;

import com.spartanullnull.otil.domain.common.exception.*;
import java.nio.file.*;
import lombok.*;
import lombok.extern.slf4j.*;
import org.springframework.http.*;
import org.springframework.web.bind.*;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    /**
     * 형식에 어긋난 입력형식오류예외 발생 시 예외처리 핸들러
     *
     * @param ex 입력형식오류예외
     * @return ErrorResponse => {에러코드,메세지,HttpStatus 를 담은 ErrorCode} + {에러발생사유를 담은 ErrorDetail}
     * @author 임지훈
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValidException(
        MethodArgumentNotValidException ex) {
        log.error("handleMethodArgumentNotValidException", ex);
        ErrorCode errorCode = ErrorCode.of(ErrorCase.DATA_ERROR);
        final ErrorResponse response = ErrorResponse.of(errorCode, ex.getBindingResult());
        return new ResponseEntity<>(response, errorCode.getStatus());
    }


    /**
     * 권한 제한 예외 발생 시 예외처리 핸들러
     *
     * @param ex 권한 제한 예외
     * @return ErrorResponse => {에러코드,메세지,HttpStatus 를 담은 ErrorCode} + {에러발생사유를 담은 ErrorDetail}
     * @author 임지훈
     */
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDeniedException(AccessDeniedException ex) {
        log.error("handleDomainException", ex);
        ErrorCode errorCode = ErrorCode.of(ErrorCase.ACCESS_DENIED);
        final ErrorResponse response = ErrorResponse.of(errorCode);
        return new ResponseEntity<>(response, errorCode.getStatus());
    }


    /**
     * 비즈니스 관련 예외 발생 시 예외처리 핸들러
     *
     * @param ex 비즈니스 도메인 예외
     * @return ErrorResponse => {에러코드,메세지,HttpStatus 를 담은 ErrorCode} + {에러발생사유를 담은 ErrorDetail}
     * @author 임지훈
     */
    @ExceptionHandler(DomainException.class)
    public ResponseEntity<ErrorResponse> handleDomainException(DomainException ex) {
        log.error("handleDomainException", ex);
        ErrorCode errorCode = ex.getErrorCode();
        ErrorDetail errorDetail = ex.getErrorDetail();
        final ErrorResponse response = ErrorResponse.of(errorCode, errorDetail);
        return new ResponseEntity<>(response, errorCode.getStatus());
    }
}