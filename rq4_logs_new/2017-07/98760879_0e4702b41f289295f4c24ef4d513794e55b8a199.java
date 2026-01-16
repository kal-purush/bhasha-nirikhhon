package com.mattvv.tips.auth;

import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoginTest {
  @Mock
  HttpServletRequest request;
  @Mock
  HttpServletResponse response;
  @Mock
  ServletConfig config;
  @Mock
  HttpSession session;
  @Mock
  ServletContext context;

  @Before
  public void before() {
    when(request.getSession()).thenReturn(session);
    when(config.getServletContext()).thenReturn(context);
    when(context.getInitParameter("tips.clientID")).thenReturn("clientid");
    when(context.getInitParameter("tips.clientSecret")).thenReturn("secret");
    when(context.getInitParameter("tips.callback")).thenReturn("http://localhost:8080/oauth");
  }

  @Test
  public void shouldPreserveLoginDestination() throws IOException, ServletException {
    when(request.getAttribute("loginDestination")).thenReturn("/tips");

    Login login = new Login();
    login.init(config);

    login.doGet(request, response);
    verify(session).setAttribute("loginDestination", "/tips");
  }

  @Test
  public void shouldDefaultToBaseLoginDestination() throws IOException, ServletException {
    when(request.getAttribute("loginDestination")).thenReturn(null);

    Login login = new Login();
    login.init(config);

    login.doGet(request, response);
    verify(session).setAttribute("loginDestination", "/");
  }
}