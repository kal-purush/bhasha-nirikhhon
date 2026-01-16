package dev.stocky37.xiv.actions.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import io.quarkus.logging.Log;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.Produces;

public class Data {

	private final static ObjectMapper yaml = new ObjectMapper(new YAMLFactory());

	@Produces
	@Named("data.actions")
	@Singleton
	@SuppressWarnings("UnstableApiUsage")
	public Map<String, JsonNode> loadActionData() {
		try {
			return yaml.readValue(
				Resources.getResource("actions.yml"),
				new TypeReference<>() {}
			);
		} catch (IllegalArgumentException | IOException e) {
			Log.info("Failed to load actions data", e);
			return new HashMap<>();
		}
	}
}