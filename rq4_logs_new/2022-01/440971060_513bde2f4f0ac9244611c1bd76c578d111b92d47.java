package com.cloudmore.exception;

/**
 * ClientProblemCode
 *
 * @author zen
 */
public enum ClientProblemCode {
    CANNOT_PARSE_EVENTTIME(400, TechnicalProblemCode.REQUEST_DATA_NOT_VALID),
    KAFKA_SEND_EXCEPTION(500, TechnicalProblemCode.UPSTREAM_SERVICE_ISSUE);


    private final int httpStatusCode;
    private final TechnicalProblemCode technicalProblemCode;

    ClientProblemCode(int httpStatusCode, TechnicalProblemCode technicalProblemCode) {
        this.httpStatusCode = httpStatusCode;
        this.technicalProblemCode = technicalProblemCode;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public TechnicalProblemCode getTechnicalProblemCode() {
        return technicalProblemCode;
    }
}