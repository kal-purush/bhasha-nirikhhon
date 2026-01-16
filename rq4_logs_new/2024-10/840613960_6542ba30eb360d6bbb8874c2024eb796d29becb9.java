package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.ReviewErrorCode;

public class ReviewCustomException extends BaseCustomException{

    public static final ReviewCustomException INVALID_DATA =
            new ReviewCustomException(ReviewErrorCode.INVALID_DATA);

    public static final ReviewCustomException NOT_FOUND =
            new ReviewCustomException(ReviewErrorCode.NOT_FOUND);

    public static final ReviewCustomException ALREADY_DATA =
            new ReviewCustomException(ReviewErrorCode.ALREADY_DATA);

    ReviewCustomException(ReviewErrorCode errorCode) { super(errorCode); }
}