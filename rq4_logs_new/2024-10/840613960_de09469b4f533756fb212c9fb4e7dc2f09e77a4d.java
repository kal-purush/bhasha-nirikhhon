package com.petmeet.api.core.exception;

import com.petmeet.api.core.exception.errorcode.CommentErrorCode;

public class CommentCustomException extends BaseCustomException{

    public static final CommentCustomException INVALID_DATA =
            new CommentCustomException(CommentErrorCode.INVALID_DATA);

    public static final CommentCustomException NOT_FOUND =
            new CommentCustomException(CommentErrorCode.NOT_FOUND);

    public static final CommentCustomException ALREADY_DATA =
            new CommentCustomException(CommentErrorCode.ALREADY_DATA);

    CommentCustomException(CommentErrorCode errorCode) { super(errorCode); }
}