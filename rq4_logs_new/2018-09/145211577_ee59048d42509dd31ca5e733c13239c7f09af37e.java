package siit.finalProject.VehicleManagement.controller;

import org.springframework.beans.factory.annotation.Autowired;
import siit.finalProject.VehicleManagement.domain.User;
import siit.finalProject.VehicleManagement.service.SecurityService;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


//@Component
public class SecurityFilter implements Filter {

    @Autowired
    private SecurityService securityService;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        User user = (User) ((HttpServletRequest) servletRequest).getSession().getAttribute("currentUser");

        securityService.setCurrentUser(user);

        String url = ((HttpServletRequest) servletRequest).getRequestURL().toString();

        if (url.contains("vehicle")) {
            if (user == null) {
                HttpServletResponse response = (HttpServletResponse) servletResponse;
                response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
                response.setHeader("Location", "/login");
                return;
            } else {
                filterChain.doFilter(servletRequest, servletResponse);
            }
        }
    }

    @Override
    public void destroy() {

    }
}
