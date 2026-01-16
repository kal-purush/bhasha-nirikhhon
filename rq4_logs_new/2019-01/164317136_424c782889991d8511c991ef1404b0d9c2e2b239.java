package com.example.venkatanarayana.apptask.model;

public class Resource<T> {
    public enum Status {
        NOT_STARTED, LOADING, SUCCESS, FAILURE;
    }

    Status status;
    T value;

    public Resource(T value, Status status) {
        this.value = value;
        this.status = status;
    }

    public static <T> Resource<T> loading(T value) {
        return new Resource<T>(value, Status.LOADING);
    }

    public static <T> Resource<T> success(T value) {
        return new Resource<T>(value, Status.SUCCESS);
    }

    public static <T> Resource<T> notStarted(T value) {
        return new Resource<T>(value, Status.NOT_STARTED);
    }

    public static <T> Resource<T> failed() {
        return new Resource<T>(null, Status.FAILURE);
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public T getValue() {
        return value;
    }
}