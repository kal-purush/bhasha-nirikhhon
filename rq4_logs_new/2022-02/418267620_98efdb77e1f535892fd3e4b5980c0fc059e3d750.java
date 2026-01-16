package dev.stocky37.xiv.actions.core;

import com.fasterxml.jackson.databind.JsonNode;
import dev.stocky37.xiv.actions.data.Ability;
import dev.stocky37.xiv.actions.json.AbilityDeserializer;
import dev.stocky37.xiv.actions.json.JsonNodeWrapper;
import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

@SuppressWarnings("UnstableApiUsage")
@ApplicationScoped
public class AbilityEnricher implements UnaryOperator<Ability> {
	private final Map<String, JsonNode> data;

	@Inject
	public AbilityEnricher(@Named("data.actions") Map<String, JsonNode> data) {
		this.data = data;
	}

	@Override
	public Ability apply(Ability ability) {
		final JsonNodeWrapper json = JsonNodeWrapper.from(data.get(ability.id()));
		if(json == null) {
			return ability;
		}
		final Ability.Builder builder = Ability.builder(ability);
		if(!json.get(AbilityDeserializer.RECAST).isMissingNode()) {
			builder.withRecast(Duration.ofMillis(json.get(AbilityDeserializer.RECAST).asLong() * 100));
		}
		return builder.build();
	}
}