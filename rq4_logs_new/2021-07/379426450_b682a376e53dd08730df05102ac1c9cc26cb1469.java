package com.google.sps.servlets;

import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.datastore.v1.Filter;
import com.google.gson.Gson;
import com.google.sps.data.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Servlet responsible for listing tasks. */
@WebServlet("/get-random-data")
public class GameDataServlet extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

    Random rand = new Random();
    long randIndex = rand.nextInt(3);

    Query<Entity> query = Query.newEntityQueryBuilder().setKind("Task").setFilter(PropertyFilter.eq("hc_id", randIndex)).build();

    QueryResults<Entity> results = datastore.run(query);

    Task task = null;
    if(results.hasNext()) {
      Entity entity = results.next();

      long id = entity.getKey().getId();
      long hc_id = entity.getLong("hc_id");
      String prompt = entity.getString("prompt");
      String actual = entity.getString("actual");
      String comparison = entity.getString("comparison");
      String source = entity.getString("source");
      long timestamp = entity.getLong("timestamp");

      task = new Task(id, hc_id, prompt, actual, comparison, source, timestamp);
    }

    Gson gson = new Gson();

    response.setContentType("application/json;");
    response.getWriter().println(gson.toJson(task));
  }
}