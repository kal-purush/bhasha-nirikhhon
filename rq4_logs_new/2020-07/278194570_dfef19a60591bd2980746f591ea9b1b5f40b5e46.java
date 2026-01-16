// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.sps.servlets;

import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.gson.Gson;
import java.io.PrintWriter;
import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/login")
public class LoginServlet extends HttpServlet {
  private static class LoginStat {
    private final boolean loggedIn;
    private final String email;

    public LoginStat(boolean loggedIn, String email) {
      this.loggedIn = loggedIn;
      this.email = email;
    }

    public static LoginStat fromUser(User user) {
      if (user == null) {
        return new LoginStat(false, "");
      }
      return new LoginStat(true, user.getEmail());
    }
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    UserService userService = UserServiceFactory.getUserService();
    
    //will return json 
    if ("json".equals(request.getParameter("alt"))) {
      String json = getUserJson(userService);
      response.setContentType("application/json");
      response.getWriter().println(json);
      return;
    }
    
    response.setContentType("text/html");
    if (userService.isUserLoggedIn()) {
      String userEmail = userService.getCurrentUser().getEmail();
      String urlToRedirectToAfterUserLogsOut = "/login";
      String logoutUrl = userService.createLogoutURL(urlToRedirectToAfterUserLogsOut);
      response.getWriter().println("<p>Hello " + userEmail + "!</p>");
      response.getWriter().println("<p>Logout <a href=\"" + logoutUrl + "\">here</a>.</p>");
    } else {
      String urlToRedirectToAfterUserLogsIn = "/login";
      String loginUrl = userService.createLoginURL(urlToRedirectToAfterUserLogsIn);

      response.getWriter().println("<p>Hello stranger.</p>");
      response.getWriter().println("<p>Login <a href=\"" + loginUrl + "\">here</a>.</p>");
    }
  }

  private String getUserJson(UserService userService) {
    LoginStat loginStat = LoginStat.fromUser(userService.getCurrentUser());
    return (new Gson()).toJson(loginStat);
  }
}