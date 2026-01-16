package by.bsuir.poit.servlets.command.impl;

import by.bsuir.poit.bean.User;
import by.bsuir.poit.context.RequestHandlerDefinition;
import by.bsuir.poit.services.AuthorizationService;
import by.bsuir.poit.services.exception.authorization.AuthorizationException;
import by.bsuir.poit.servlets.command.RequestHandler;
import by.bsuir.poit.servlets.command.RequestMethod;
import by.bsuir.poit.utils.AuthorizationUtils;
import by.bsuir.poit.utils.RedirectUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Paval Shlyk
 * @since 06/11/2023
 */
@RequestHandlerDefinition(urlPatterns = RedirectUtils.REGISTRATION_PAGE, method = RequestMethod.POST)
@RequiredArgsConstructor
public class RegistrationHandler implements RequestHandler {
private static final Logger LOGGER = LogManager.getLogger(RegistrationHandler.class);
private final AuthorizationService authorizationService;

@Override
public void accept(HttpServletRequest request, HttpServletResponse response) throws Exception {
      try {
	    String password = (String) request.getAttribute(AuthorizationUtils.PASSWORD);
	    User user = AuthorizationUtils.parseUser(request);
	    authorizationService.register(user, password);
	    response.sendRedirect(RedirectUtils.AUTHORIZATION_PAGE);
      } catch (AuthorizationException e) {
	    processRegistrationException(e, response);
      } catch (Exception e) {
	    LOGGER.warn("Failed to register user from ip {}", request.getRemoteAddr());
	    response.sendRedirect(RedirectUtils.ERROR_PAGE);
      }
}

private void processRegistrationException(AuthorizationException exception, HttpServletResponse response) {
      try {
	    response.getWriter().write(exception.getMessage());
      } catch (Exception e) {
	    LOGGER.error(e.getMessage());
	    throw new IllegalStateException(e);
      }
}
}