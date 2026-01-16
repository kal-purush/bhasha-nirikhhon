package stirling.software.SPDF.config.security;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * Filter that intercepts requests and redirects to the password setup page when the system is in
 * first-run state (no users configured).
 */
@Slf4j
@Component
@Order(1) // High priority to run before authentication filters
public class FirstRunPasswordFilter extends OncePerRequestFilter {

    private final FirstRunPasswordSetup firstRunPasswordSetup;

    private static final String[] ALLOWED_PATHS = {
        "/setup/password", "/css/", "/js/", "/images/", "/fonts/", "/assets/", "/error"
    };

    @Autowired
    public FirstRunPasswordFilter(FirstRunPasswordSetup firstRunPasswordSetup) {
        this.firstRunPasswordSetup = firstRunPasswordSetup;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getServletPath();

        // If first-run setup is needed and this is not an allowed path, redirect to setup
        if (firstRunPasswordSetup.isFirstRunSetupNeeded() && !isAllowedPath(path)) {
            log.debug("Redirecting to password setup: {}", path);
            response.sendRedirect(request.getContextPath() + "/setup/password");
            return;
        }

        // Otherwise continue with the filter chain
        filterChain.doFilter(request, response);
    }

    /** Check if the requested path is allowed without password setup */
    private boolean isAllowedPath(String path) {
        for (String allowedPath : ALLOWED_PATHS) {
            if (path.startsWith(allowedPath)) {
                return true;
            }
        }
        return false;
    }
}