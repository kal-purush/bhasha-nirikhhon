package by.bsuir.poit.servlets;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import java.security.Principal;

/**
 * @author Paval Shlyk
 * @since 07/11/2023
 */
public class UserPrincipalHttpServletRequest extends HttpServletRequestWrapper {
private final Principal principal;

/**
 * Constructs a request object wrapping the given request.
 *
 * @param request the {@link HttpServletRequest} to be wrapped.
 * @throws IllegalArgumentException if the request is null
 */
public UserPrincipalHttpServletRequest(HttpServletRequest request, Principal principal) {
      super(request);
      this.principal = principal;
}

@Override
public Principal getUserPrincipal() {
      return principal;
}
}