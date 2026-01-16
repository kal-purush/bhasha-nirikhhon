package ru.netology;

public class Request {
    private String path;
    private String method;

    public Request(String method, String path) {
        this.method = method;
        this.path = path;
    }

    public static Request addRequest(String method, String path) {
        if (method != null) {
            return new Request(method, path);
        }
        return null;
    }

    public String getPath() {
        return path;
    }

    public String getMethod() {
        return method;
    }
}