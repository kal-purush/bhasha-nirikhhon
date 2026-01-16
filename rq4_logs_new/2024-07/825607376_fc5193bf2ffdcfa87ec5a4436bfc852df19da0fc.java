package refooding.api.common.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum ExceptionCode {

    // 404
    NOT_FOUND_EXCHANGE(HttpStatus.NOT_FOUND, "존재하지 않는 교환글 입니다"),
    NOT_FOUND_REGION(HttpStatus.NOT_FOUND, "존재하지 않는 지역입니다"),

    // 401
    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "접근 권한이 없습니다"),

    // 500
    INTERNAL_SERVER_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "서버 에러가 발생했습니다");

    private final HttpStatus status;
    private final String message;

    ExceptionCode(HttpStatus status, String message) {
        this.status = status;
        this.message = message;
    }

}