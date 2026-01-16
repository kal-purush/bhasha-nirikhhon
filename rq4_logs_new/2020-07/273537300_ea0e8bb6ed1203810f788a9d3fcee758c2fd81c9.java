// Copyright 2020 Google LLC
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.sps.utility.AuthenticationUtility;
import com.google.sps.utility.GmailUtility;
import java.io.IOException;
import java.util.List;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Serves selected information from the User's Gmail Account. TODO: Create Servlet Utility to handle
 * common functions (Issue #26) TODO: Create abstract class to handle authentication with each
 * request (Issue #37)
 */
@WebServlet("/gmail")
public class GmailServlet extends HttpServlet {

  /**
   * Returns messageIds from the user's Gmail account
   *
   * @param request Http request from the client. Should contain idToken and accessToken
   * @param response 403 if user is not authenticated, list of messageIds otherwise
   * @throws IOException if an issue arises while processing the request
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    // get Google credential object. Ensure it is valid - otherwise return an error to client
    Credential googleCredential = AuthenticationUtility.getGoogleCredential(request);
    if (googleCredential == null) {
      response.sendError(403, AuthenticationUtility.ERROR_403);
      return;
    }

    // Get messageIds from Gmail
    Gmail gmailService = GmailUtility.getGmailService(googleCredential);
    List<Message> messages = GmailUtility.listUserMessages(gmailService, "");

    // convert messageIds to JSON object and print to response
    Gson gson = new Gson();
    String messageJson = gson.toJson(messages);

    response.setContentType("application/json");
    response.getWriter().println(messageJson);
  }
}