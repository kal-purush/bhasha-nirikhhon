package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.MeetingErrorCode;

public class MeetingCustomException extends BaseCustomException{

    public static final MeetingCustomException INVALID_DATA =
            new MeetingCustomException(MeetingErrorCode.INVALID_DATA);

    public static final MeetingCustomException NOT_FOUND =
            new MeetingCustomException(MeetingErrorCode.NOT_FOUND);

    public static final MeetingCustomException ALREADY_DATA =
            new MeetingCustomException(MeetingErrorCode.ALREADY_DATA);

    MeetingCustomException(MeetingErrorCode errorCode) { super(errorCode); }
}