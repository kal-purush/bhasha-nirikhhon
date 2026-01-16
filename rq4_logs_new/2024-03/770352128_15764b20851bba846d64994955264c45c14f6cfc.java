package moofarm.ssuckssuck.global.utils;

import moofarm.ssuckssuck.global.exception.UserNotFoundException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class SecurityUtils {
    public static Long getCurrentUserId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null) throw UserNotFoundException.EXCEPTION;

        return Long.valueOf(authentication.getName());
    }
}