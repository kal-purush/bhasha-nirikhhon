package soma.edupiuser.web.exception;

import org.springframework.http.HttpStatus;

public enum ErrorEnum {
    // 400
    TOKEN_EXPIRE(HttpStatus.BAD_REQUEST, "AC-400001", "토큰이 만료되었습니다."),

    META_SERVER_EXCEPTION(HttpStatus.INTERNAL_SERVER_ERROR, "AC-500001", "Meta server Exception"),

    ;

    private final HttpStatus httpStatus;
    private final String code;
    private final String detail;

    ErrorEnum(HttpStatus httpStatus, String code, String details) {
        this.httpStatus = httpStatus;
        this.code = code;
        this.detail = details;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public String getCode() {
        return code;
    }

    public String getDetail() {
        return detail;
    }

}