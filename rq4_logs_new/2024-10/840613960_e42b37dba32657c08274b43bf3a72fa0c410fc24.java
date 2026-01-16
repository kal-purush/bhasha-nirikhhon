package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.UserErrorCode;

public class UserCustomException extends BaseCustomException{

    public static final UserCustomException INVALID_DATA =
            new UserCustomException(UserErrorCode.INVALID_DATA);

    public static final UserCustomException NOT_FOUND =
            new UserCustomException(UserErrorCode.NOT_FOUND);

    public static final UserCustomException ALREADY_DATA =
            new UserCustomException(UserErrorCode.ALREADY_DATA);

    UserCustomException(UserErrorCode errorCode) { super(errorCode); }
}