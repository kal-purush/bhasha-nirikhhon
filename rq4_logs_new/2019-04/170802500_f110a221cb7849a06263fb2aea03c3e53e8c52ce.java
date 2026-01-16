package com.google.codeu.servlets;

import com.google.codeu.data.Datastore;
import com.google.codeu.data.Event;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Servlet that will initiate whenever a user goes to an event tag page. */
@WebServlet("/event-tags")
public class EventTagServlet extends HttpServlet {
  private Datastore datastore;

  /** Initializes a new Datastore object. */
  @Override
  public void init() {
    datastore = new Datastore();
  }

  /** Retrieve all tags from each event created. */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    List<Event> events = datastore.getAllEvents();
    List<String> tags = new ArrayList<>();
    
    Iterator it = events.iterator();
    while (it.hasNext()) {
      Event e = (Event) it.next();
      String description = e.getDescription();

      String regex = "(#\\w+)";

      // Retrieve all hashtags in the descriptions
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(description);

      while (m.find()) {
        tags.add(m.group(1));
      }
    }
    Gson gson = new Gson();
    String json = gson.toJson(tags);
    response.getWriter().println(json);
  }
}