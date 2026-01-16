package com.mattvv.tips.auth;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogoutTest {
  @Mock
  HttpServletRequest request;
  @Mock
  HttpServletResponse response;
  @Mock
  HttpSession session;

  @Test
  public void shouldClearSessionWhenSessionExists() throws Exception {
    when(request.getSession(false)).thenReturn(session);

    Logout logout = new Logout();
    logout.doGet(request, response);

    verify(session).invalidate();
    verify(request).getSession();
    verify(response).sendRedirect("/");

  }

  @Test
  public void shouldRedirectWhenNoSessionExists() throws Exception {
    when(request.getSession(false)).thenReturn(null);
    Logout logout = new Logout();
    logout.doGet(request, response);

    verifyZeroInteractions(session);
    verify(request).getSession();
    verify(response).sendRedirect("/");
  }
}