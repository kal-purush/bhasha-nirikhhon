package algo.trading.starter.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.springframework.web.client.RestClient;

/**
 * Provides thread-safe management and caching of {@link RestClient} instances keyed by refresh
 * tokens.
 */
public class RestClientProvider {
  private final ConcurrentHashMap<String, RestClient> clients = new ConcurrentHashMap<>();
  private final Function<String, RestClient> restClientFactory;
  private final RefreshTokenProvider refreshTokenProvider;

  /** Constructor. */
  public RestClientProvider(
      Function<String, RestClient> restClientFactory, RefreshTokenProvider refreshTokenProvider) {
    this.restClientFactory = restClientFactory;
    this.refreshTokenProvider = refreshTokenProvider;
  }

  /**
   * Retrieves or creates a REST client associated with the current refresh token.
   *
   * @return a non-null RestClient instance associated with current refresh token
   */
  public RestClient getRestClient() {
    String refreshToken = refreshTokenProvider.getRefreshToken();
    return clients.computeIfAbsent(refreshToken, restClientFactory);
  }
}