package com.share.share_scripts.exception;

import com.share.share_scripts.exception.handler.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ReceiptNotFoundException extends RuntimeException {
    private final ErrorCode errorCode;
}