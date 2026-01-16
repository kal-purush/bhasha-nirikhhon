package com.supervisor.util.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

abstract public class AbstractResult {
    private String field;
    private String code;
    private String message;

    public abstract boolean isSuccess();

    public abstract boolean isError();

    AbstractResult(String field, String code, String message) {
        this.field = field;
        this.code = code;
        this.message = message;
    }

    @JsonProperty("field")
    public String getField() { return this.field; }

    @JsonProperty("code")
    public String getCode() { return code; }

    @JsonProperty("message")
    public String getMessage() { return message; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractResult that = (AbstractResult) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(code, that.code) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, code, message);
    }
}