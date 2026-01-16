package ru.otus.dataprocessor;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import ru.otus.model.Measurement;

import java.io.IOException;

public class FileDeserializer extends StdDeserializer<Measurement> {
    protected FileDeserializer(Class<?> vc) {
        super(vc);
    }

    protected FileDeserializer() {
        this(null);
    }

    @Override
    public Measurement deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        String name = node.get("name").asText();
        double value = node.get("value").doubleValue();
        return new Measurement(name, value);
    }
}