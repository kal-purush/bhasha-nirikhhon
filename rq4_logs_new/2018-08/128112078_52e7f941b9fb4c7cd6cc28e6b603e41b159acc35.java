package application.algorithm;

import application.configuration.CredentialAuthorizer;
import application.configuration.SheetsConnector;
import application.utility.SheetsRetriever;
import com.google.api.client.auth.oauth2.Credential;
import java.io.IOException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class AlgorithmConfiguration {
  @Bean
  ResponseToAlgorithmTransformer responseToAlgorithmTransformer(CredentialAuthorizer credentialAuthorizer, SheetsConnector sheetsConnector)
      throws IOException {
    Credential authorize = credentialAuthorizer.authorize();
    SheetsRetriever sheetsRetriever = new SheetsRetriever(sheetsConnector.connect(authorize));
    return new ResponseToAlgorithmTransformer(sheetsRetriever);
  }

  @Bean
  AlgorithmSettings algorithmSettings(ResponseToAlgorithmTransformer responseToAlgorithmTransformer) {
    return new AlgorithmSettings(responseToAlgorithmTransformer);
  }

  @Bean
  AlgorithmExecutor algorithmExecutor(AlgorithmSettings algorithmSettings) {
    return new AlgorithmExecutor(algorithmSettings);
  }
}