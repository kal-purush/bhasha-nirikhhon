package data.config.properties.plugin;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class IndexingConfigurationProperties {

  private String chunkSize;
  private String parallelizationSize;

  private String preserveTimestamps;
  private String performRedirects;

  private String mongoInstances;
  private String mongoPortNumber;
  private String mongoDbName;
  private String mongoRedirectsDbName;
  private String mongoUsername;
  private String mongoPassword;
  private String mongoAuthDB;
  private String mongoUseSSL;
  private String mongoReadPreference;
  private String mongoPoolSize;
  private String mongoApplicationName;

  private String solrInstances;
  private String zookeeperInstances;
  private String zookeeperPortNumber;
  private String zookeeperChroot;
  private String zookeeperDefaultCollection;

}