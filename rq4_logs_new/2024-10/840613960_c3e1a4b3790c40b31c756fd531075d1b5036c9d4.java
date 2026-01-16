package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.AuthErrorCode;

public class AuthCustomException extends BaseCustomException{

    public static final AuthCustomException INVALID_TOKEN =
            new AuthCustomException(AuthErrorCode.INVALID_TOKEN);

    public static final AuthCustomException EXPIRED_TOKEN =
            new AuthCustomException(AuthErrorCode.EXPIRED_TOKEN);

    public static final AuthCustomException MISSING_TOKEN =
            new AuthCustomException(AuthErrorCode.MISSING_TOKEN);

    public static final AuthCustomException TOKEN_REFRESH_FAILED =
            new AuthCustomException(AuthErrorCode.TOKEN_REFRESH_FAILED);

    public static final AuthCustomException INVALID_CREDENTIALS =
            new AuthCustomException(AuthErrorCode.INVALID_CREDENTIALS);

    public static final AuthCustomException UNAUTHORIZED_ACCESS =
            new AuthCustomException(AuthErrorCode.UNAUTHORIZED_ACCESS);

    public static final AuthCustomException INSUFFICIENT_PERMISSIONS =
            new AuthCustomException(AuthErrorCode.INSUFFICIENT_PERMISSIONS);

    public static final AuthCustomException INVALID_AUTHORIZATION_HEADER =
            new AuthCustomException(AuthErrorCode.INVALID_AUTHORIZATION_HEADER);

    public static final AuthCustomException INVALID_OAUTH_PROVIDER =
            new AuthCustomException(AuthErrorCode.INVALID_OAUTH_PROVIDER);

    AuthCustomException(AuthErrorCode errorCode) { super(errorCode); }
}