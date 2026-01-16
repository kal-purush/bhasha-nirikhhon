package dev.stocky37.xiv.actions.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.net.URI;
import java.util.StringJoiner;
import java.util.function.Function;
import javax.ws.rs.core.UriBuilder;

public abstract class JsonNodeDeserializer<T> extends StdDeserializer<T>
	implements Function<JsonNode, T> {

	private final UriBuilder uriBuilder;

	protected JsonNodeDeserializer(Class<?> vc, String baseUri) {
		super(vc);
		this.uriBuilder = UriBuilder.fromUri(baseUri);
	}

	@Override
	public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		return apply(p.getCodec().readTree(p));
	}

	protected URI getUri(JsonNode node, String path) {
		return prefixUri(get(node, path).asText());
	}

	protected JsonNode get(JsonNode node, String path) {
		return get(node, path.split("\\."));
	}

	protected JsonNode get(JsonNode node, String... paths) {
		final StringJoiner joiner = new StringJoiner("/", "/", "");
		for(final var path : paths) {
			joiner.add(path);
		}
		return node.at(joiner.toString());
	}

	protected URI prefixUri(String relativePath) {
		return uriBuilder.clone().path(relativePath).build();
	}
}