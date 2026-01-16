package groupone.userservice.security;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

import javax.servlet.http.HttpServletRequest;

public class JwtTokenInterceptor implements RequestInterceptor {

    @Override
    public void apply(RequestTemplate template) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        System.out.println("in interceptor: "+authentication.getCredentials());
        String token = null;
        if (authentication != null) {
            token = (String) authentication.getCredentials();
        }

        System.out.println("in interceptor: "+token);
        // Add the JWT token to the request headers
        template.header("Authorization", "Bearer " + token);
    }
}