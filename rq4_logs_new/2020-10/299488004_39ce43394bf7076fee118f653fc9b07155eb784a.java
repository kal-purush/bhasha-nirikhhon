package com.cartas.jaktani.dto;

public class ParentResponse {
    private Object data;

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public ParentResponse(Object data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ParentResponse{");
        sb.append("data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}