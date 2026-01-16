package software.sebastian.oposiciones.config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import java.io.IOException;

public class CustomLogoutSuccessHandler implements LogoutSuccessHandler {

    @Override
    public void onLogoutSuccess(HttpServletRequest request,
                                HttpServletResponse response,
                                Authentication authentication)
            throws IOException, ServletException {

        String emailChanged = request.getParameter("emailChanged");

        if ("true".equals(emailChanged)) {
            response.sendRedirect("/login?logout&msg=email-cambiado");
        } else {
            response.sendRedirect("/login?logout");
        }
    }
}