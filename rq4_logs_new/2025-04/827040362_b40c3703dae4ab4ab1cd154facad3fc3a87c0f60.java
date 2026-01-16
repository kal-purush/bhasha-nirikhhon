package org.iromu.ai.mcp;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * <pre>
 * graphql:
 *   schemas:
 *     - classpath:schema/user.graphqls
 *     - https://example.com/schema.graphql
 *  </pre>
 */
@Configuration
@ConfigurationProperties(prefix = "graphql")
@Data
public class GraphQLConfig {

	private List<String> schemas; // URLs or paths to GraphQL schema files

}