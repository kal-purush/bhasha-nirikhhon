package com.google.sps.servlets;

import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Servlet that returns HTML that contains the page view count. */
@WebServlet("/phrase")
public class PhraseServlet extends HttpServlet {

  private String phrase = "Hello World";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    
    response.setContentType("text/html;");
    response.getWriter().println("<h1>Phrase</h1>");
    response.getWriter().println("<p>" + phrase + "</p>");
  }
}