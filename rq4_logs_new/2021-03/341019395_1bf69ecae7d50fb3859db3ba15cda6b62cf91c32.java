package com.google.sps.servlets;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Servlet that returns HTML that shows my favorite colors */
@WebServlet("/Favorite-Colors")

public class FavoriteColors extends HttpServlet {

  @Override

  
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ArrayList<String> colors = new ArrayList<String>();
    colors.add("1. Blue ");
    colors.add("2. Light Green");
    colors.add("3. Light Pink");
    Gson gson=new Gson();
    String json=gson.toJson(colors);
    response.setContentType("application/json;");
    response.getWriter().println(json);
  }
}