package br.com.zup.TaDentro.jwt.filter;

import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

public class FiltroDeAutorizacao extends BasicAuthenticationFilter {

    public FiltroDeAutorizacao(AuthenticationManager authenticationManager) {
        super(authenticationManager);
    }

    public FiltroDeAutorizacao(AuthenticationManager authenticationManager, AuthenticationEntryPoint authenticationEntryPoint) {
        super(authenticationManager, authenticationEntryPoint);
    }
}