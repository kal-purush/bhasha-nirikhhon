package com.bahubba.bahubbabookclub.util;

import com.bahubba.bahubbabookclub.model.entity.Reader;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Utility class for security-related methods
 */
public class SecurityUtil {
    public static Reader getCurrentUserDetails() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            return (Reader) authentication.getPrincipal();
        }
        return null;
    }
}