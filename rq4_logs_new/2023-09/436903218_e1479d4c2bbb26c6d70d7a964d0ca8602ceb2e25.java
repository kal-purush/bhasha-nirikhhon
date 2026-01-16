package de.caritas.cob.statisticsservice.config.apiclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import de.caritas.cob.statisticsservice.userstatisticsservice.generated.web.UserStatisticsControllerApi;
import de.caritas.cob.statisticsservice.userstatisticsservice.generated.ApiClient;

@Component
@RequiredArgsConstructor
public class UserStatisticsApiControllerFactory {

  @Value("${user.statistics.service.api.url}")
  private String userStatisticsServiceApiUrl;

  @Autowired
  private RestTemplate restTemplate;

  private final ObjectMapper objectMapper;

  public UserStatisticsControllerApi createControllerApi() {
    var apiClient = new ApiClient(restTemplate).setBasePath(this.userStatisticsServiceApiUrl);
    return new UserStatisticsControllerApi(apiClient);
  }
}