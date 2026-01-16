package fi.vm.sade.koodisto.audit;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Fillteri joka lisää lokituksen kontekstiin auditlokituksen tarvitsemia
 * tietoja.
 *
 * Aktivointi: web.xml
 */
public class AuditFilter extends OncePerRequestFilter {

    private static final String KEY_REMOTE_ADDR = "remoteAddr";
    private static final String KEY_SESSION = "session";
    private static final String KEY_USER_AGENT = "userAgent";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        insertIntoMDC(request);
        try {
            filterChain.doFilter(request, response);
        } finally {
            clearMDC();
        }
    }

    private static void insertIntoMDC(HttpServletRequest request) {
        MDC.put(KEY_REMOTE_ADDR, getRemoteAddr(request));
        MDC.put(KEY_SESSION, request.getSession().getId());
        MDC.put(KEY_USER_AGENT, request.getHeader("User-Agent"));
    }

    private static void clearMDC() {
        MDC.remove(KEY_REMOTE_ADDR);
        MDC.remove(KEY_SESSION);
        MDC.remove(KEY_USER_AGENT);
    }

    private static String getRemoteAddr(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        return xForwardedFor != null ? xForwardedFor : request.getRemoteAddr();
    }

}