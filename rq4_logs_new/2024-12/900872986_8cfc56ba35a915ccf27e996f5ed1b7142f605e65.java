package com.group_component.master_gateway.request;

public class RegisterRequest {
    private final String email;
    private final String password;
    private final String confirm;

    public RegisterRequest(String email, String password, String confirm) {
        this.email = email;
        this.password = password;
        this.confirm = confirm;
    }

    public String getEmail() {
        return this.email;
    }

    public String getPassword() {
        return this.password;
    }

    public String getConfirm() {
        return this.confirm;
    }
}

