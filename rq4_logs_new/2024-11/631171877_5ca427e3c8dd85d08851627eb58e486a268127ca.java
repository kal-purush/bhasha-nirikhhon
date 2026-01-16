package net.trellisframework.data.elastic.payload;

import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResult;
import co.elastic.clients.json.*;
import co.elastic.clients.util.ObjectBuilder;

import java.util.function.Function;
import java.util.function.Supplier;

@JsonpDeserializable
public class SirenMsearchResponse<TDocument> extends MultiSearchResult<TDocument> {

    private SirenMsearchResponse(Builder<TDocument> builder) {
        super(builder.took(0));

    }

    public static <TDocument> SirenMsearchResponse<TDocument> of(Function<Builder<TDocument>, ObjectBuilder<SirenMsearchResponse<TDocument>>> fn) {
        return fn.apply(new Builder<>()).build();
    }

    public static class Builder<TDocument> extends MultiSearchResult.AbstractBuilder<TDocument, Builder<TDocument>> implements ObjectBuilder<SirenMsearchResponse<TDocument>> {
        @Override
        protected Builder<TDocument> self() {
            return this;
        }

        public SirenMsearchResponse<TDocument> build() {
            _checkSingleUse();
            this.took(0);
            return new SirenMsearchResponse<TDocument>(this);
        }
    }

    public static <TDocument> JsonpDeserializer<SirenMsearchResponse<TDocument>> createMsearchResponseDeserializer(JsonpDeserializer<TDocument> tDocumentDeserializer) {
        return ObjectBuilderDeserializer.createForObject((Supplier<Builder<TDocument>>) Builder::new, op -> SirenMsearchResponse.setupMsearchResponseDeserializer(op, tDocumentDeserializer));
    }

    public static final JsonpDeserializer<SirenMsearchResponse<Object>> _DESERIALIZER = JsonpDeserializer.lazy(() -> createMsearchResponseDeserializer(new NamedDeserializer<>("co.elastic.clients:Deserializer:_global.msearch.TDocument")));

    protected static <TDocument> void setupMsearchResponseDeserializer(ObjectDeserializer<Builder<TDocument>> op, JsonpDeserializer<TDocument> tDocumentDeserializer) {
        MultiSearchResult.setupMultiSearchResultDeserializer(op, tDocumentDeserializer);
    }

}