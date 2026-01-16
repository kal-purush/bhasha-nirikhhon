package com.github.ssullivan;

import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ApplicationStartupTest {
  private static final AppConfig config = new AppConfig();
  static {
    config.setRedisConfig(new RedisConfig());
  }

  private static final DropwizardAppExtension<AppConfig> RULE = new DropwizardAppExtension<>(ApiApplication.class, config);

  @Test
  public void testHealthCheck() {
    Client client = new JerseyClientBuilder().build();
    try {
      Response response = client.target(String.format("http://localhost:%d/", RULE.getLocalPort()))
          .request()
          .get();

      MatcherAssert.assertThat(response.getStatus(), Matchers.equalTo(200));
    }
    finally {
      client.close();
    }
  }
}