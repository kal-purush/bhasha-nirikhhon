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

package com.google.sps.utility;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

/** Contains methods for common servlet operations */
public final class ServletUtility {
  public static final String APPLICATION_NAME = "PUT NAME HERE";

  private ServletUtility() {}

  /**
   * Helper method to get a cookie from an HttpServletRequest
   *
   * @param request HttpServletRequest that contains desired cookie
   * @param cookieName name of desired cookie. Case sensitive
   * @return Cookie if found, null if not found or if duplicates present
   */
  public static Cookie getCookie(HttpServletRequest request, String cookieName) {
    List<Cookie> cookies =
        Arrays.stream(request.getCookies())
            .filter((Cookie c) -> c.getName().equals(cookieName))
            .collect(Collectors.toList());

    if (cookies.isEmpty()) {
      System.out.println("Cookie not found");
      return null;
    }

    // If more than one cookies are found, it is ambiguous as to which one to return.
    // This is unexpected - duplicate cookies are usually blocked by the browser (especially
    // when they are user set).
    if (cookies.size() > 1) {
      System.out.printf("%d duplicate cookies with name %s\n", cookies.size(), cookieName);
      cookies.forEach(c -> System.out.println(c.getValue()));

      return null;
    }

    return cookies.get(0);
  }

  /**
   * Helper function to create an authorization header for an outgoing HTTP request Does NOT check
   * if the access token provides correct permissions for the request Does NOT check if the
   * userToken was valid. Use verifyUserToken to verify userId validity
   *
   * @param request contains cookies for a userToken and accessToken
   * @return Value of the "Authorization" header. Null if authentication is invalid or accessToken
   *     was not found
   */
  public static String generateAuthorizationHeader(HttpServletRequest request) {
    // If accessToken cannot be found, return null
    Cookie authCookie = getCookie(request, "accessToken");
    if (authCookie == null) {
      return null;
    }

    // Otherwise, return the accessToken as a Bearer token for the Authorization Header
    return "Bearer " + authCookie.getValue();
  }
}