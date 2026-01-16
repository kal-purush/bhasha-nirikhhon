package app.bpartners.geojobs.endpoint.rest.readme.monitor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Builder
@Component
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties("readme.monitor")
public class ReadmeMonitorConf {
  private String apiKey;
  private String version;
  private String url;
  private String name;
  private boolean development;
}