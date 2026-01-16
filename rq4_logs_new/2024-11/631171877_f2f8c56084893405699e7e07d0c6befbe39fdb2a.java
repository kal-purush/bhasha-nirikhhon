package net.trellisframework.data.elastic.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.JsonEndpoint;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.endpoints.EndpointWithResponseMapperAttr;
import co.elastic.clients.transport.endpoints.SimpleEndpoint;
import co.elastic.clients.util.ApiTypeHelper;
import co.elastic.clients.util.ObjectBuilder;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SirenElasticsearchClient extends ElasticsearchClient {

    public static final SimpleEndpoint<SearchRequest, ?> _ENDPOINT = new SimpleEndpoint<>("es/search",


            request -> "POST",

            request -> {
                final int _index = 1 << 0;

                int propsSet = 0;

                if (ApiTypeHelper.isDefined(request.index())) propsSet |= _index;

                if (propsSet == 0) {
                    return "/_search";
                }
                if (propsSet == (_index)) {
                    StringBuilder buf = new StringBuilder();
                    buf.append("/siren/");
                    SimpleEndpoint.pathEncode(String.join(",", request.index()), buf);
                    buf.append("/_search");
                    return buf.toString();
                }
                throw SimpleEndpoint.noPathTemplateFound("path");

            },

            request -> {
                Map<String, String> params = new HashMap<>();
                final int _index = 1 << 0;

                int propsSet = 0;

                if (ApiTypeHelper.isDefined(request.index())) propsSet |= _index;

                if (propsSet == 0) {
                }
                if (propsSet == (_index)) {
                    params.put("index", String.join(",", request.index()));
                }
                return params;
            },

            request -> {
                Map<String, String> params = new HashMap<>();
                params.put("typed_keys", "true");
                if (request.df() != null) {
                    params.put("df", request.df());
                }
                if (request.preFilterShardSize() != null) {
                    params.put("pre_filter_shard_size", String.valueOf(request.preFilterShardSize()));
                }
                if (request.minCompatibleShardNode() != null) {
                    params.put("min_compatible_shard_node", request.minCompatibleShardNode());
                }
                if (request.forceSyntheticSource() != null) {
                    params.put("force_synthetic_source", String.valueOf(request.forceSyntheticSource()));
                }
                if (request.lenient() != null) {
                    params.put("lenient", String.valueOf(request.lenient()));
                }
                if (request.routing() != null) {
                    params.put("routing", request.routing());
                }
                if (request.ignoreUnavailable() != null) {
                    params.put("ignore_unavailable", String.valueOf(request.ignoreUnavailable()));
                }
                if (request.allowNoIndices() != null) {
                    params.put("allow_no_indices", String.valueOf(request.allowNoIndices()));
                }
                if (request.analyzer() != null) {
                    params.put("analyzer", request.analyzer());
                }
                if (request.ignoreThrottled() != null) {
                    params.put("ignore_throttled", String.valueOf(request.ignoreThrottled()));
                }
                if (request.maxConcurrentShardRequests() != null) {
                    params.put("max_concurrent_shard_requests", String.valueOf(request.maxConcurrentShardRequests()));
                }
                if (request.allowPartialSearchResults() != null) {
                    params.put("allow_partial_search_results", String.valueOf(request.allowPartialSearchResults()));
                }
                if (ApiTypeHelper.isDefined(request.expandWildcards())) {
                    params.put("expand_wildcards", request.expandWildcards().stream().map(v -> v.jsonValue()).collect(Collectors.joining(",")));
                }
                if (request.preference() != null) {
                    params.put("preference", request.preference());
                }
                if (request.analyzeWildcard() != null) {
                    params.put("analyze_wildcard", String.valueOf(request.analyzeWildcard()));
                }
                if (request.scroll() != null) {
                    params.put("scroll", request.scroll()._toJsonString());
                }
                if (request.searchType() != null) {
                    params.put("search_type", request.searchType().jsonValue());
                }
                if (request.ccsMinimizeRoundtrips() != null) {
                    params.put("ccs_minimize_roundtrips", String.valueOf(request.ccsMinimizeRoundtrips()));
                }
                if (request.q() != null) {
                    params.put("q", request.q());
                }
                if (request.defaultOperator() != null) {
                    params.put("default_operator", request.defaultOperator().jsonValue());
                }
                if (request.requestCache() != null) {
                    params.put("request_cache", String.valueOf(request.requestCache()));
                }
                if (request.batchedReduceSize() != null) {
                    params.put("batched_reduce_size", String.valueOf(request.batchedReduceSize()));
                }
                return params;

            }, SimpleEndpoint.emptyMap(), true, SearchResponse._DESERIALIZER);

    public SirenElasticsearchClient(ElasticsearchTransport transport) {
        super(transport, null);
    }

    public SirenElasticsearchClient(ElasticsearchTransport transport, @Nullable TransportOptions transportOptions) {
        super(transport, transportOptions);
    }

    public <TDocument> SearchResponse<TDocument> sirenSearch(Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> fn, Class<TDocument> tDocumentClass) throws IOException, ElasticsearchException {
        return sirenSearch(fn.apply(new SearchRequest.Builder()).build(), tDocumentClass);
    }

    public <TDocument> SearchResponse<TDocument> sirenSearch(SearchRequest request, Class<TDocument> tDocumentClass) throws IOException, ElasticsearchException {
        @SuppressWarnings("unchecked") JsonEndpoint<SearchRequest, SearchResponse<TDocument>, ErrorResponse> endpoint = (JsonEndpoint<SearchRequest, SearchResponse<TDocument>, ErrorResponse>) _ENDPOINT;
        endpoint = new EndpointWithResponseMapperAttr<>(endpoint, "co.elastic.clients:Deserializer:_global.search.TDocument", getDeserializer(tDocumentClass));
        return this.transport.performRequest(request, endpoint, this.transportOptions);
    }

    public final <TDocument> SearchResponse<TDocument> sirenSearch(Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> fn, Type tDocumentType) throws IOException, ElasticsearchException {
        return sirenSearch(fn.apply(new SearchRequest.Builder()).build(), tDocumentType);
    }

    public <TDocument> SearchResponse<TDocument> sirenSearch(SearchRequest request, Type tDocumentType) throws IOException, ElasticsearchException {
        @SuppressWarnings("unchecked") JsonEndpoint<SearchRequest, SearchResponse<TDocument>, ErrorResponse> endpoint = (JsonEndpoint<SearchRequest, SearchResponse<TDocument>, ErrorResponse>) _ENDPOINT;
        endpoint = new EndpointWithResponseMapperAttr<>(endpoint, "co.elastic.clients:Deserializer:_global.search.TDocument", getDeserializer(tDocumentType));
        return this.transport.performRequest(request, endpoint, this.transportOptions);
    }

    public final Long sirenCount(Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> fn) throws IOException, ElasticsearchException {
        return sirenCount(fn.apply(new SearchRequest.Builder().size(0)).build());
    }

    public Long sirenCount() throws IOException, ElasticsearchException {
        return sirenCount(new SearchRequest.Builder().size(0).build());
    }

    public Long sirenCount(SearchRequest request) throws IOException, ElasticsearchException {
        JsonEndpoint<SearchRequest, SearchResponse, ErrorResponse> endpoint = (JsonEndpoint<SearchRequest, SearchResponse, ErrorResponse>) _ENDPOINT;
        return Optional.ofNullable(this.transport.performRequest(request, endpoint, this.transportOptions)).map(ResponseBody::hits).map(HitsMetadata::total).map(TotalHits::value).orElse(0L);
    }
}