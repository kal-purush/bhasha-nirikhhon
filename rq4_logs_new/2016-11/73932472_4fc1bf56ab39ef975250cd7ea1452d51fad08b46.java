package com.jtl.common.exception;

/**
 * Created by Administrator on 2016/11/16 0016.
 */
public class BusinessException extends RuntimeException {

    public BusinessException() {

    }

    public BusinessException(String message) {
        super(message);

    }

    public BusinessException(Throwable cause) {
        super(cause);

    }

    public BusinessException(String message, Throwable cause) {
        super(message, cause);

    }

}