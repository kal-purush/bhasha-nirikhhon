package com.google.codeu.servlets;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.google.gson.JsonArray;
import com.google.codeu.utils.CsvLoader;

@WebServlet("/moviechart")
public class ChartServlet extends HttpServlet {

  private JsonArray movieInfoArray;

  @Override
  public void init() {
    Scanner scanner = new Scanner(getServletContext().getResourceAsStream("/WEB-INF/movies.csv"));
    try {
      movieInfoArray = CsvLoader.csvToJson(scanner);
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }

   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
     response.setContentType("application/json");
     response.getOutputStream().println(movieInfoArray.toString());
   }
}