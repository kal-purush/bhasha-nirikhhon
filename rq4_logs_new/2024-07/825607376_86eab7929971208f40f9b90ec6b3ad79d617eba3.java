package refooding.api.common.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import refooding.api.common.exception.CustomException;
import refooding.api.common.exception.ExceptionResponse;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.METHOD_NOT_ALLOWED;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler{

    @ExceptionHandler(CustomException.class)
    public ResponseEntity<ExceptionResponse> handleCustomException(CustomException e) {
        log.warn("[CustomException] {}", e.getMessage(), e);

        ExceptionResponse response = ExceptionResponse.of(e.getExceptionCode());

        return ResponseEntity.status(response.status()).body(response);
    }

    /**
     * 해당 api(url)에 지원하지 않는 http 메서드 요청시 발생하는 예외
     */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ExceptionResponse> handleHttpRequestMethodNotSupportedException(
            HttpRequestMethodNotSupportedException e
    ) {
        log.warn("[HttpRequestMethodNotSupportedException] {}", e.getMessage(), e);

        ExceptionResponse response = ExceptionResponse.of(METHOD_NOT_ALLOWED, "지원하지 않는 HTTP 메서드입니다");

        return ResponseEntity.status(response.status()).body(response);
    }

    /**
     * HTTP Body 파싱이 제대로 되지 않았을 때 발생하는 예외
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ExceptionResponse> handleHttpMessageNotReadableException(
            HttpMessageNotReadableException e
    ) {
        log.warn("[HttpMessageNotReadableException] {}", e.getMessage(), e);

        ExceptionResponse response = ExceptionResponse.of(BAD_REQUEST, "요청 본문을 읽을 수 없습니다");

        return ResponseEntity.status(response.status()).body(response);
    }

    /***
     * 요청 파라미터에서 사용되는 enum을 converter가 변환하지 못한 경우 발생하는 예외
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ExceptionResponse> handle(MethodArgumentTypeMismatchException e) {
        log.warn("[MethodArgumentTypeMismatchException] {}", e.getMessage(), e);

        ExceptionResponse response = ExceptionResponse.of(BAD_REQUEST, "요청 데이터 타입이 잘못되었습니다");

        return ResponseEntity.status(response.status()).body(response);
    }

    /***
     * 예기치 못한 서버 예외
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ExceptionResponse> handleException(final Exception e) {
        log.warn("[Exception] {}", e.getMessage(), e);

        ExceptionResponse response = ExceptionResponse.of(HttpStatus.INTERNAL_SERVER_ERROR, "예기치 못한 서버 오류가 발생하였습니다");

        return ResponseEntity.status(response.status()).body(response);
    }

}