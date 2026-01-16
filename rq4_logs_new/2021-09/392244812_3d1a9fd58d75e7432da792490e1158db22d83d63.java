package com.tinkoff.edu.app.repository;

import com.tinkoff.edu.app.dictionary.LoanResponseStatus;
import com.tinkoff.edu.app.model.LoanRequest;
import com.tinkoff.edu.app.model.LoanResponse;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Loan repository
 */
public class ArrayCalcRepository implements LoanCalcRepository {
    private LoanRequest[] requestsArray;

    @Override
    public LoanResponse save(LoanRequest loanRequest, LoanResponseStatus loanResponseStatus) {
        if (Arrays.stream(requestsArray).noneMatch(Objects::isNull)) {
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < requestsArray.length; i++) {
            if (requestsArray[i] == null) {
                requestsArray[i] = loanRequest;
                break;
            }
        }

        return new LoanResponse(UUID.randomUUID(), loanRequest, loanResponseStatus);
    }

    public ArrayCalcRepository() {
        requestsArray = new LoanRequest[5];
    }

    public LoanRequest[] getRequestsArray() {
        return requestsArray;
    }
}