package com.redhat.console.integrations;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.util.json.JsonObject;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthenticationException;
// Import httpclient classes.
// Currently form version 4, which depends on camel-http package.
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;

/*
 * Sets Authorization header as a Basic authentication
 * reading the password from X-Insight-Token metadata.
 *
 * Camel HTTP supports basic auth, however it cannot be dynamically
 * set, unless toD (dynamic) would be used.
 */
public class BasicAuthenticationProcessor implements Processor {

    private String username;

    public BasicAuthenticationProcessor(String username) {
        this.username = username;
    }

    public void process(Exchange exchange) throws AuthenticationException {
        Message in = exchange.getIn();

        JsonObject metadata = in.getHeader("metadata", JsonObject.class);
        String password = metadata.getString("X-Insight-Token");
        if (password == null || password.equals("")) {
            return;
        }

        Credentials credentials = new UsernamePasswordCredentials(this.username, password);

        BasicScheme basicScheme = new BasicScheme();
        final HttpRequest request = new BasicHttpRequest("POST", "/");

        Header header = basicScheme.authenticate(credentials, request, new BasicHttpContext());

        in.setHeader("Authorization", header.getValue());
    }

}