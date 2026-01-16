package com.customs.network.fdapn.exception;

import lombok.Getter;

@Getter
public enum ErrorResCodes{
    INVALID_REFERENCE_ID("1000","Invalid Reference id "),
    NOT_FOUND("1001","Not found "),
    RECORD_NOT_FOUND("1002","Record not found "),
    USER_NOT_FOUND("1003","User not found "),
    SOMETHING_WENT_WRONG("1004","Unexpected Error"),
    EMPTY_DETAILS("1005", "Empty details"),
    INVALID_DETAILS("1006","Invalid details"),
    CONVERSION_FAILURE("1007","Data conversion failure")
    ;

    private final String errorCode;
    private final String errorMsg;

    ErrorResCodes(String errorCode, String errorMsg) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }
}