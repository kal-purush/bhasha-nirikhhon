package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.PushNotificationErrorCode;
import com.petmeet.api.core.exception.errorcode.UserErrorCode;

public class PushNotificationCustomException extends BaseCustomException{

    public static final PushNotificationCustomException INVALID_DATA =
            new PushNotificationCustomException(PushNotificationErrorCode.INVALID_DATA);

    public static final PushNotificationCustomException NOT_FOUND =
            new PushNotificationCustomException(PushNotificationErrorCode.NOT_FOUND);

    public static final PushNotificationCustomException ALREADY_DATA =
            new PushNotificationCustomException(PushNotificationErrorCode.ALREADY_DATA);

    PushNotificationCustomException(PushNotificationErrorCode errorCode) { super(errorCode); }
}