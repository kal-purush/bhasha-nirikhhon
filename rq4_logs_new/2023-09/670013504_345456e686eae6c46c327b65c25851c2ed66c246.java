package com.grievance.Configuration;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * A custom Servlet Filter implementation to
 * perform pre and post-processing of HTTP requests and responses.
 * This filter can be used to add custom
 * logic before and after a request reaches the controller.
 */
//@Configuration
//@Order(Ordered.HIGHEST_PRECEDENCE)
public class UserFilter implements Filter {
/**
 * logger.
 */
  private static final Logger LOGGER =
          LoggerFactory.getLogger(UserFilter.class);

  /**
   * Initializes the custom filter.
   *
   * @param filterConfig The FilterConfig
   * object containing filter configuration details.
   * @throws ServletException If an error occurs during filter initialization.
   */
  @Override
  public void init(final FilterConfig filterConfig) throws ServletException {
      LOGGER.info("Initiating Custom Filter");
  }

  /**
   * Performs filtering of HTTP requests and responses.
   *
   * @param request  The ServletRequest object representing
   * the incoming HTTP request.
   * @param response The ServletResponse object representing
   * the HTTP response to be sent.
   * @param chain    The FilterChain for executing the next filter in the chain.
   * @throws IOException      If an I/O error occurs
   * while processing the request or response.
   * @throws ServletException If an error occurs during filter processing.
   */
  @Override
  public void doFilter(
          final ServletRequest request,
          final ServletResponse response,
          final FilterChain chain)
          throws IOException, ServletException {
      System.out.println("Request received at: " + System.currentTimeMillis());

      chain.doFilter(request, response);
      HttpServletRequest httpServletRequest = (HttpServletRequest)request;
      System.out.println(httpServletRequest.getHeader("email"));
      System.out.println("Response sent at: " + System.currentTimeMillis());
  }

  /**
   * Destroys the filter and releases any resources held by it.
   * This method is called by the servlet
   * container when the filter is taken out of service.
   */
  @Override
  public void destroy() {
  }
}