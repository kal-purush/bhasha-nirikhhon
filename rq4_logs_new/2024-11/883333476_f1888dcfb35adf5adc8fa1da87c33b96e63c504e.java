package org.rentifytools.security.utils;

import lombok.experimental.UtilityClass;
import org.rentifytools.entity.Role;
import org.rentifytools.security.AuthInfo;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Set;

@UtilityClass
public class SecurityUtils {
    public static AuthInfo getCurrentUser() {
        return (AuthInfo) SecurityContextHolder.getContext().getAuthentication();
    }

    public static Long getCurrentUserId() {
        AuthInfo authInfo = getCurrentUser();
        if (authInfo == null) {
            throw new IllegalStateException("User is not authorized");
        }
        return authInfo.getUserId();
    }

    public static String getCurrentUserEmail() {
        AuthInfo authInfo = getCurrentUser();
        if (authInfo == null) {
            throw new IllegalStateException("User is not authorized");
        }
        return authInfo.getEmail();
    }

    public static Set<Role> getCurrentUserRoles() {
        AuthInfo authInfo = getCurrentUser();
        if (authInfo == null) {
            throw new IllegalStateException("User is not authorized");
        }
        return authInfo.getRoles();
    }

    public static boolean hasRole(String role) {
        AuthInfo authInfo = getCurrentUser();
        if (authInfo == null) {
            throw new IllegalStateException("User is not authorized");
        }
        return authInfo.getRoles().stream()
                .anyMatch(r -> r.getTitle().equalsIgnoreCase(role));
    }

    public static boolean isAuthenticated() {
        AuthInfo authInfo = getCurrentUser();
        return authInfo != null && authInfo.isAuthenticated();
    }
}