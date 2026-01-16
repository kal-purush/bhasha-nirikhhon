package com.es.phoneshop.cart;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ErrorMap {
    private Map<String, ArrayList<String>> errorMap;

    public ErrorMap() {
        errorMap = new HashMap<>();
    }

    public void addError(String key, String error) {
        if (errorMap.get(key) == null) {
            ArrayList<String> errors = new ArrayList<>();
            errors.add(error);
            errorMap.put(key, errors);
        } else {
            errorMap.get(key).add(error);
        }
    }

    public Map<String, ArrayList<String>> getErrorMap() {
        return errorMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorMap errorMap1 = (ErrorMap) o;
        return Objects.equals(errorMap, errorMap1.errorMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorMap);
    }
}