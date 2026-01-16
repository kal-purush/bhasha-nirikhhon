package pl.dchruscinski.config.filter;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// @Component
// @Order(1)
public class LoggerFilter implements Filter {
    public static final Logger logger = LoggerFactory.getLogger(LoggerFilter.class);

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (request instanceof HttpServletRequest httpRequest){
            logger.info("doFilter(): [HttpServletRequest] method: {}, URI: {}", httpRequest.getMethod(),
                    httpRequest.getRequestURI());
        }

        chain.doFilter(request, response);

        if (response instanceof HttpServletResponse httpResponse){
            logger.info("doFilter(): [HttpServletResponse] content type: {}", httpResponse.getContentType());
        }
    }
}