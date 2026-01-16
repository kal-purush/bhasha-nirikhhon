package myDentist.security;

import org.springframework.security.authentication.AuthenticationTrustResolver;
import org.springframework.security.authentication.AuthenticationTrustResolverImpl;
import org.springframework.security.core.context.SecurityContextHolder;

import myDentist.model.User;

public class SecurityUtils {

    private static AuthenticationTrustResolver authenticationTrustResolver = new AuthenticationTrustResolverImpl();

    public static boolean isAnonymous()
    {
        return authenticationTrustResolver.isAnonymous(
            SecurityContextHolder.getContext().getAuthentication() );
    }

    public static boolean isAuthenticated()
    {
        return !isAnonymous();
    }

    public static User getUser()
    {
        return isAuthenticated() ? (User) SecurityContextHolder.getContext()
            .getAuthentication()
            .getPrincipal() : null;
    }

}