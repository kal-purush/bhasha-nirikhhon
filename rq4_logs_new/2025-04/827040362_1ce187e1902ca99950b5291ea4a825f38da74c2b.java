package org.iromu.ai.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ReactorClientHttpRequestFactory;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class RestClientConfiguration {

	@Bean
	public RestClientCustomizer customizer() {
		return builder -> {
			HttpClient httpClient = HttpClient.create()
				.responseTimeout(Duration.ofSeconds(60)) // Read timeout
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000) // Connect timeout
				.doOnConnected(conn -> conn.addHandlerLast(new ReadTimeoutHandler(60, TimeUnit.SECONDS)) // read
																											// timeout
																											// per
																											// operation
					.addHandlerLast(new WriteTimeoutHandler(60, TimeUnit.SECONDS)) // write
																					// timeout
																					// per
																					// operation
			);
			builder.requestFactory(new ReactorClientHttpRequestFactory(httpClient));
		};
	}

}