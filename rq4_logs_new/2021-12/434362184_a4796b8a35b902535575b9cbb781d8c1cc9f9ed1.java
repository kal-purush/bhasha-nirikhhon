package br.com.senior.springbook.service.profile;

import br.com.senior.springbook.model.Tokens;
import org.springframework.stereotype.Service;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Service
public interface ProfileService extends Filter {

    @Override
    public void doFilter(ServletRequest request,//
                         ServletResponse response,//
                         FilterChain chain)//
            throws IOException, ServletException ;
    }

}