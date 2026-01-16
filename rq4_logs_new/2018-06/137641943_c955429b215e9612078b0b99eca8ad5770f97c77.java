package io.su0.json.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import io.su0.json.parser.walker.HandlerStorage;
import io.su0.json.parser.walker.JsonTreeWalker;
import io.su0.json.path.matcher.JsonPathMatcher;
import io.su0.json.path.parsing.Facade;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;

public abstract class AbstractJsonParser<Meta> {

    protected final HandlerStorage<Meta> handlerStorage;

    public AbstractJsonParser() {
        this.handlerStorage = new HandlerStorage<>();
    }

    public void handleInS(InputStream inputStream, Meta meta) throws IOException {
        JsonTreeWalker.walk(inputStream, handlerStorage, meta);
    }

    public void handlePs(JsonParser parser, Meta meta) throws IOException {
        JsonTreeWalker.walk(parser, handlerStorage, meta);
    }

    protected <Value> void add(String expr, BiConsumer<Meta, Value> consumer) {
        JsonPathMatcher matcher = Facade.parse(expr);
        handlerStorage.addHandler(
                (path, jsonToken) -> matcher.match(path) && jsonToken.isScalarValue(),
                (parser, path, token, meta) -> consumer.accept(meta, (Value) convert(parser, token))
        );
    }

    protected <NestedMeta> void add(String expr, BiConsumer<Meta, NestedMeta> consumer, AbstractJsonParserOfType<NestedMeta> nested) {
        JsonPathMatcher matcher = Facade.parse(expr);
        handlerStorage.addHandler(
                (path, token) -> matcher.match(path) && JsonToken.FIELD_NAME == token,
                (parser, path, token, meta) -> {
                    NestedMeta parse = nested.parse(parser);
                    consumer.accept(meta, parse);
                }
        );
    }

    private static Object convert(JsonParser parser, JsonToken token) throws IOException {
        switch (token) {
            case VALUE_STRING:
                return parser.getValueAsString();
            case VALUE_FALSE:
            case VALUE_TRUE:
                return parser.getBooleanValue();
            case VALUE_NUMBER_INT:
                return parser.getIntValue();
            case VALUE_NUMBER_FLOAT:
                return parser.getDoubleValue();
            case VALUE_NULL:
                return null;
            default:
                throw new IllegalStateException();
        }
    }
}