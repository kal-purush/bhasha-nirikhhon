package com.google.codeu.servlets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Scanner;

/**
 * Returns cinema location data as a JSON array, e.g. [{"lat": 38.4404675, "lng": -122.7144313}]
 */
@WebServlet("/cinema-data")
public class CinemaLocationServlet extends HttpServlet {
  private JsonArray cinemaLocationsArray;

  @Override
  public void init() {
    cinemaLocationsArray = new JsonArray();
    Gson gson = new Gson();
    Scanner scanner = new Scanner(getServletContext().getResourceAsStream("/WEB-INF/cinemaLocations.csv"));
    while(scanner.hasNextLine()) {
      String line = scanner.nextLine();
      String[] cells = line.split(",");

      double lat = Double.parseDouble(cells[1]);
      double lng = Double.parseDouble(cells[2]);

      cinemaLocationsArray.add(gson.toJsonTree(new CinemaLocation(lat, lng)));
    }
    scanner.close();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    response.getOutputStream().println(cinemaLocationsArray.toString());
  }

  private static class CinemaLocation {
    double lat;
    double lng;

    private CinemaLocation(double lat, double lng) {
      this.lat = lat;
      this.lng = lng;
    }
  }
}