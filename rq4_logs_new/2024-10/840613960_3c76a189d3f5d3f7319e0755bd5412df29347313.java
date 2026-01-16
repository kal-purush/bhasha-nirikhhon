package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.AttendanceRecordErrorCode;

public class AttendanceRecordCustomException extends BaseCustomException{

    public static final AttendanceRecordCustomException INVALID_DATA =
            new AttendanceRecordCustomException(AttendanceRecordErrorCode.INVALID_DATA);

    public static final AttendanceRecordCustomException NOT_FOUND =
            new AttendanceRecordCustomException(AttendanceRecordErrorCode.NOT_FOUND);

    public static final AttendanceRecordCustomException ALREADY_DATA =
            new AttendanceRecordCustomException(AttendanceRecordErrorCode.ALREADY_DATA);

    AttendanceRecordCustomException(AttendanceRecordErrorCode errorCode) { super(errorCode); }
}