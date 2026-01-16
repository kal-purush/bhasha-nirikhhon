package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.PetErrorCode;

public class PetCustomException extends BaseCustomException{

    public static final PetCustomException INVALID_DATA =
            new PetCustomException(PetErrorCode.INVALID_DATA);

    public static final PetCustomException NOT_FOUND =
            new PetCustomException(PetErrorCode.NOT_FOUND);

    public static final PetCustomException ALREADY_DATA =
            new PetCustomException(PetErrorCode.ALREADY_DATA);

    PetCustomException(PetErrorCode errorCode) { super(errorCode); }
}