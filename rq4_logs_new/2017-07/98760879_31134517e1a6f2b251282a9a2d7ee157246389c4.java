package com.mattvv.tips;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IndexControllerTest {
  @Mock
  HttpServletRequest request;
  @Mock
  HttpServletResponse response;
  @Mock
  PrintWriter writer;

  @Test
  public void itShouldPrintOutHelloWorld() throws IOException {

    when(response.getWriter()).thenReturn(writer);
    IndexController index = new IndexController();
    index.doGet(request, response);
    verify(writer).println("Hello, world - Flex Servlet");
  }
}