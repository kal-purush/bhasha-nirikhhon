package com.fatepet.global.exception;

import com.fatepet.global.response.ResponseCode;
import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class BaseException extends RuntimeException {

    private final int statusCode;
    private final String message;

    public BaseException(ResponseCode responseCode) {
        super(responseCode.getMessage());
        this.statusCode = responseCode.getStatusCode();
        this.message = responseCode.getMessage();
    }

    public BaseException(String message){
        super(message);
        this.statusCode = HttpStatus.BAD_REQUEST.value();
        this.message = message;
    }

    public BaseException(int status, String message) {
        super(message);
        this.statusCode = status;
        this.message = message;
    }

}