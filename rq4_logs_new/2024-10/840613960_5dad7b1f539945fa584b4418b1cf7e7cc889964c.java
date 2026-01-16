package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.MeetingMemberErrorCode;

public class MeetingMemberCustomException extends BaseCustomException{

    public static final MeetingMemberCustomException INVALID_DATA =
            new MeetingMemberCustomException(MeetingMemberErrorCode.INVALID_DATA);

    public static final MeetingMemberCustomException NOT_FOUND =
            new MeetingMemberCustomException(MeetingMemberErrorCode.NOT_FOUND);

    public static final MeetingMemberCustomException ALREADY_DATA =
            new MeetingMemberCustomException(MeetingMemberErrorCode.ALREADY_DATA);

    MeetingMemberCustomException(MeetingMemberErrorCode errorCode) { super(errorCode); }
}