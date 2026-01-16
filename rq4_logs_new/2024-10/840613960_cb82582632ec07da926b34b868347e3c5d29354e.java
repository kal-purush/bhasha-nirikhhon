package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.SocialAccountErrorCode;

public class SocialAccountCustomException extends BaseCustomException{

    public static final SocialAccountCustomException INVALID_DATA =
            new SocialAccountCustomException(SocialAccountErrorCode.INVALID_DATA);

    public static final SocialAccountCustomException NOT_FOUND =
            new SocialAccountCustomException(SocialAccountErrorCode.NOT_FOUND);

    public static final SocialAccountCustomException ALREADY_DATA =
            new SocialAccountCustomException(SocialAccountErrorCode.ALREADY_DATA);

    SocialAccountCustomException(SocialAccountErrorCode errorCode) { super(errorCode); }
}