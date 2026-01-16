/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.codeu.servlets;

import java.io.IOException;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.codeu.data.Datastore;
import com.google.codeu.data.User;
import com.google.codeu.data.Event;

import org.apache.commons.lang3.StringUtils;

/** Handles fetching and saving event data. */
@WebServlet("/event-form")
public class EventFormServlet extends HttpServlet {

  private Datastore datastore;

  @Override
  public void init() {
    datastore = new Datastore();
  }

  /*Responds with details of a specific event.*/
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    response.setContentType("text/html");

    String eventName = request.getParameter("eventName");

    if (StringUtils.isEmpty(eventName)) {
      // Request is invalid, return empty response
      return;
    }

    /*Event eventData = datastore.getEvent(eventName);
    response.getOutputStream().println();*/
  }

  /** Stores a new {@link Event}. */
  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException {

      UserService userService = UserServiceFactory.getUserService();
      if (!userService.isUserLoggedIn()) {
        response.sendRedirect("/index.html");
        return;
      }

      String eventName = request.getParameter("eventName");
      String description = request.getParameter("description");
      String organizerNames = request.getParameter("organizerNames");
      String eventDate = request.getParameter("eventDate");
      String eventTime = request.getParameter("eventTime");
      String location = request.getParameter("location");

      Event event = new Event(eventName, description, organizerNames,eventDate,eventTime,location);
      datastore.storeEvent(event);
    }

}