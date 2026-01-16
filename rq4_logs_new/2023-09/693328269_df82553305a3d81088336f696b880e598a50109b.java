package com.fromscratch.users;

import com.fromscratch.users.application.authentication.AuthenticationPage;

public class App {
    
    private AuthenticationPage authenticationPage;
    
    public App(AuthenticationPage authenticationPage) {
        this.authenticationPage = authenticationPage;
    }

    public String defaultPage() {
        return authenticationPage.getAuthenticationForm();
    }
}