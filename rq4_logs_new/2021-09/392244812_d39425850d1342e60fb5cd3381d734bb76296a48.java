package com.tinkoff.edu.app.repository;

import com.tinkoff.edu.app.dictionary.LoanResponseStatus;
import com.tinkoff.edu.app.model.LoanRequest;
import com.tinkoff.edu.app.model.LoanResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class HashMapLoanCalcRepository implements LoanCalcRepository {

    private final HashMap<UUID, LoanRequest> requestsMap;
    private final HashMap<UUID, LoanResponse> responsesMap;

    public HashMapLoanCalcRepository() {
        requestsMap = new HashMap<>();
        responsesMap = new HashMap<>();
    }

    public Map<UUID, LoanRequest> getRequests() {
        return Collections.unmodifiableMap(requestsMap);
    }

    public Map<UUID, LoanResponse> getResponses() {
        return Collections.unmodifiableMap(responsesMap);
    }

    @Override
    public LoanResponse save(LoanRequest loanRequest, LoanResponseStatus loanResponseStatus) {
        if (loanRequest == null)
            throw new NullPointerException("Данные по заявке отсутствуют");

        UUID nextResponseId = UUID.randomUUID();
        LoanResponse loanResponse = new LoanResponse(loanRequest.getRequestId(), loanRequest, loanResponseStatus);

        requestsMap.put(loanRequest.getRequestId(), loanRequest);
        responsesMap.put(nextResponseId, loanResponse);

        return loanResponse;
    }
}