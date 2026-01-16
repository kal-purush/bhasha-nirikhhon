package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.DeviceErrorCode;

public class DeviceCustomException extends BaseCustomException{

    public static final DeviceCustomException INVALID_DATA =
            new DeviceCustomException(DeviceErrorCode.INVALID_DATA);

    public static final DeviceCustomException NOT_FOUND =
            new DeviceCustomException(DeviceErrorCode.NOT_FOUND);

    public static final DeviceCustomException ALREADY_DATA =
            new DeviceCustomException(DeviceErrorCode.ALREADY_DATA);

    DeviceCustomException(DeviceErrorCode errorCode) { super(errorCode); }
}