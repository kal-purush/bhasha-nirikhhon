package com.mattvv.tips;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mattvv.tips.models.Team;
import java.io.IOException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TeamsTest {

  @Mock
  HttpServletRequest request;
  @Mock
  HttpServletResponse response;
  @Mock
  RequestDispatcher dispatcher;
  @Mock
  ServletConfig config;
  @Mock
  ServletContext context;

  @Test
  public void shouldLoadTeams() throws IOException, ServletException {
    when(config.getServletContext()).thenReturn(context);
    when(request.getRequestDispatcher("/base.jsp")).thenReturn(dispatcher);
    Teams teams = new Teams();
    teams.init(config);
    teams.doGet(request, response);
    verify(context).setAttribute("teams", Team.getTeams());
    verify(request).setAttribute("page", "teams");
    verify(request).getRequestDispatcher("/base.jsp");
    verify(dispatcher).forward(request, response);
  }
}