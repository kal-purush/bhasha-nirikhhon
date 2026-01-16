package groupone.userservice.security;


import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import javax.security.auth.Subject;
import java.util.Collection;
import java.util.List;

public class LoginUserAuthentication implements Authentication {
    private final int userId;
    private final String email;
    private final List<GrantedAuthority> authorities;
    private boolean isAuthenticated;

    public LoginUserAuthentication(int userId, List<GrantedAuthority> authorities, boolean isAuthenticated, String email) {
        this.userId = userId;
        this.authorities = authorities;
        this.isAuthenticated = isAuthenticated;
        this.email = email;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return this.authorities;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getDetails() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return null;
    }

    @Override
    public boolean isAuthenticated() {
        return this.isAuthenticated;
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {

    }

    @Override
    public String getName() {
        return null;
    }

    public int getUserID() {
        return this.userId;
    }

    @Override
    public boolean implies(Subject subject) {
        return Authentication.super.implies(subject);
    }
}