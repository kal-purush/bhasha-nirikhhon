package com.ms.order.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILED = "FAILED";
    public static final String ERROR = "ERROR";

    public static final String IN_PROGRESS = "IN_PROGRESS";
    public static final String IN_QUEUE = "REQUEST_IN_QUEUE";

    /* Error codes Starts */
    public static final String ERROR_MESSAGE_SENDING_TO_QUEUE = "ERROR_MESSAGE_SENDING_TO_QUEUE";
    public static final String ERROR_MESSAGE_RECEIVING_FROM_QUEUE = "ERROR_MESSAGE_RECEIVING_FROM_QUEUE";

    /* Constants */
    public static final int RETRY_DELAY = 3000;
}