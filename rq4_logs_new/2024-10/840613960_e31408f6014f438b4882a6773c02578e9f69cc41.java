package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.PetTypeErrorCode;

public class PetTypeCustomException extends BaseCustomException{

    public static final PetTypeCustomException INVALID_DATA =
            new PetTypeCustomException(PetTypeErrorCode.INVALID_DATA);

    public static final PetTypeCustomException NOT_FOUND =
            new PetTypeCustomException(PetTypeErrorCode.NOT_FOUND);

    public static final PetTypeCustomException ALREADY_DATA =
            new PetTypeCustomException(PetTypeErrorCode.ALREADY_DATA);

    PetTypeCustomException(PetTypeErrorCode errorCode) { super(errorCode); }
}