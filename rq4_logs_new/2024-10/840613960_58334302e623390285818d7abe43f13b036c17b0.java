package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.MeetingMarkErrorCode;

public class MeetingMarkCustomException extends BaseCustomException{

    public static final MeetingMarkCustomException INVALID_DATA =
            new MeetingMarkCustomException(MeetingMarkErrorCode.INVALID_DATA);

    public static final MeetingMarkCustomException NOT_FOUND =
            new MeetingMarkCustomException(MeetingMarkErrorCode.NOT_FOUND);

    public static final MeetingMarkCustomException ALREADY_DATA =
            new MeetingMarkCustomException(MeetingMarkErrorCode.ALREADY_DATA);

    MeetingMarkCustomException(MeetingMarkErrorCode errorCode) { super(errorCode); }
}