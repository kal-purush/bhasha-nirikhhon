package thorpe.luke.ufc.fantasy.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class PropertiesConfig {

  @Value("${jwt.token.secret}")
  private String jwtTokenSecret;

  @Value("${heroku.https.enabled}")
  private Boolean herokuHttpsEnabled;

  public String getJwtTokenSecret() {
    return jwtTokenSecret;
  }

  public Boolean getHerokuHttpsEnabled() {
    return herokuHttpsEnabled;
  }
}