package com.mattvv.tips;

import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Provides;
import com.google.inject.servlet.ServletModule;
import com.mattvv.tips.auth.Login;
import com.mattvv.tips.auth.Logout;
import com.mattvv.tips.auth.OAuth;

import java.util.Arrays;
import java.util.Collection;
import javax.servlet.ServletContext;

class Routes extends ServletModule {
  @Override
  protected void configureServlets() {
    serve("/").with(Index.class);
    serve("/login").with(Login.class);
    serve("/logout").with(Logout.class);
    serve("/oauth").with(OAuth.class);
  }

  @Provides
  HttpTransport providesTransport() {
    return new NetHttpTransport();
  }

  @Provides
  GoogleAuthorizationCodeFlow providesFlow() {
    final Collection<String> SCOPES = Arrays.asList("email", "profile");
    final JsonFactory JSON_FACTORY = new JacksonFactory();

    return new GoogleAuthorizationCodeFlow.Builder(
        providesTransport(),
        JSON_FACTORY,
        servletContext().getInitParameter("tips.clientID"),
        servletContext().getInitParameter("tips.clientSecret"),
        SCOPES).build();
  }

  @VisibleForTesting
  ServletContext servletContext() {
    return getServletContext();
  }
}