package io.su0.json.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.su0.json.path.matcher.JsonPathMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;

public class AbstractJsonParserWithBuilder<Type, TypeBuilder> implements Parser<Type> {



    protected final Supplier<TypeBuilder> builderCreator;
    protected final Function<TypeBuilder, Type> buildFunction;

    public AbstractJsonParserWithBuilder(
            Supplier<TypeBuilder> builderCreator,
            Function<TypeBuilder, Type> buildFunction
    ) {
        this.builderCreator = builderCreator;
        this.buildFunction = buildFunction;
    }

    @Override
    public Type parse(InputStream inputStream) throws IOException {
        TypeBuilder accumulator = builderCreator.get();



        ObjLongConsumer<TypeBuilder> longValueHandler;







        return buildFunction.apply(accumulator);

    }
}