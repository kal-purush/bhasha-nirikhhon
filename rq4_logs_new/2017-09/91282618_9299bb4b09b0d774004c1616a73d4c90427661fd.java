package com.vincent.example.config;

import com.vincent.example.authorization.Service.ExampleAuthenticationUserDetailsService;
import com.vincent.example.authorization.filter.ExampleAuthenticationFilter;
import com.vincent.example.authorization.manager.TokenManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created : vincent
 * Date : 2017/8/3 下午12:01
 * Email : wangxiao@wafersystems.com
 */


@Configurable
@EnableWebSecurity
public class ApplicationSecurity extends WebSecurityConfigurerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(ApplicationSecurity.class);

  private final TokenManager tokenManager;

  @Autowired
  public ApplicationSecurity(TokenManager tokenManager) {
    this.tokenManager = tokenManager;
  }

  @Override
  protected void configure(AuthenticationManagerBuilder builder) throws Exception {
    builder.authenticationProvider(preAuthenticationProvider());
  }

  private AuthenticationProvider preAuthenticationProvider() {
    PreAuthenticatedAuthenticationProvider provider = new PreAuthenticatedAuthenticationProvider();
    provider.setPreAuthenticatedUserDetailsService(new ExampleAuthenticationUserDetailsService(tokenManager));
    return provider;
  }


  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.authorizeRequests().antMatchers("/tokens", "/swagger-ui.html", "/webjars/**",
      "/swagger-resources/**", "/v2/**").permitAll()
      .anyRequest().authenticated()
//      .and().csrf().csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())

      .and().sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
      .and().csrf().disable();

    // 认证转换器
    http.exceptionHandling().authenticationEntryPoint(authenticationEntryPoint());
    http.addFilter(headerAuthenticationFilter());
    http.formLogin().successHandler(authenticationSuccessHandler());
    http.formLogin().failureHandler(authenticationFailureHandler());
  }

  /**
   * 处理未授权.
   *
   * @return AuthenticationEntryPoint
   */
  @Bean
  public AuthenticationEntryPoint authenticationEntryPoint() {
    return (request, response, authException) -> response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Bean
  public SimpleUrlAuthenticationSuccessHandler authenticationSuccessHandler() {
    return new SimpleUrlAuthenticationSuccessHandler() {
      @Override
      public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
                                          Authentication authentication) throws IOException,
        ServletException {
        clearAuthenticationAttributes(request);
      }
    };
  }

  @Bean
  public SimpleUrlAuthenticationFailureHandler authenticationFailureHandler() {
    return new SimpleUrlAuthenticationFailureHandler() {
      @Override
      public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
                                          AuthenticationException exception) throws IOException,
        ServletException {
        super.onAuthenticationFailure(request, response, exception);
      }
    };
  }

  @Bean
  public ExampleAuthenticationFilter headerAuthenticationFilter() throws Exception {
    return new ExampleAuthenticationFilter(authenticationManager());
  }


}