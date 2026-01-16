package org.iromu.ai.autoconfigure;

import org.iromu.ai.config.RestClientConfig;
import org.iromu.ai.config.WebClientConfig;
import org.iromu.ai.config.WebClientProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * SpringAiWebfluxAutoConfiguration is an auto-configuration class for setting up default
 * configurations and beans related to chat-based services in a Spring WebFlux
 * environment. This configuration class ensures that required beans are registered only
 * if they are not already present in the application context.
 *
 * Primary Responsibilities: - Auto-configures the {@code OllamaController} bean if it is
 * missing in the context. - Provides a default implementation of {@code OllamaController}
 * using the {@code DefaultOllamaController} class.
 *
 * Dependency Injection: - Injects optional build properties to display application build
 * information. - Injects application name and model configuration properties. - Requires
 * an implementation of {@code OllamaService} to handle the business logic around chat
 * services.
 *
 * Annotations: - {@code @AutoConfiguration}: Indicates that this is a Spring Boot
 * auto-configuration class. - {@code @Bean}: Declares a method for defining and
 * registering a bean in the application context. - {@code @ConditionalOnMissingBean}:
 * Ensures that the bean is only created if no matching bean of the same type exists in
 * the context.
 */
@AutoConfiguration
@Import({ WebClientProperties.class, WebClientConfig.class, RestClientConfig.class })
public class SpringAiCommonAutoConfiguration {

}