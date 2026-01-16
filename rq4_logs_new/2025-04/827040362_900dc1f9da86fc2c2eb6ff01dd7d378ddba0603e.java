package org.iromu.ai.gateway;

import lombok.extern.slf4j.Slf4j;
import org.iromu.ai.controller.OllamaController;
import org.iromu.ai.model.ChatRequest;
import org.iromu.ai.model.ChatStreamResponse;
import org.iromu.ai.model.ModelTag;
import org.iromu.ai.model.TagsResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@RestController
@Slf4j
@CrossOrigin(origins = "*")
@RequestMapping({"/api", "/v1"})
public class OllamaGatewayController implements OllamaController {


    private final List<String> urls;

    private final WebClient.Builder webClientBuilder;
    private Flux<MetaTagResponse> tags;

    public OllamaGatewayController(AiConfig aiConfig, WebClient.Builder webClientBuilder) {
        this.urls = aiConfig.getUrls();
        this.webClientBuilder = webClientBuilder;
        discover();
    }

    private void discover() {
        this.tags = Flux.fromIterable(urls)
                .flatMap(url -> {
                    log.info("{}/api/tags", url);
                    return webClientBuilder.baseUrl(url).build()
                            .get()
                            .uri("/api/tags")
                            .accept(MediaType.APPLICATION_JSON)
                            .exchangeToMono(response -> response.bodyToMono(TagsResponse.class))
                            .onErrorReturn(new TagsResponse(new ArrayList<>()))
                            .map(tagsResponse -> new MetaTagResponse(url, tagsResponse));
                }).cache();
    }

    private TagsResponse aggregateResponses(List<MetaTagResponse> responses) {
        List<ModelTag> models = new ArrayList<>();
        for (MetaTagResponse response : responses) {
            if (response != null && response.tagsResponse().models() != null)
                models.addAll(response.tagsResponse().models());
        }
        log.info("models {}", models);
        return new TagsResponse(models);
    }

    public Mono<TagsResponse> getTags() {
        return this.tags.collectList()
                .map(this::aggregateResponses)
                .doOnNext(o -> log.info("{}", o));
    }

    public Flux<ChatStreamResponse> chat(@RequestBody ChatRequest request) {
        log.info("{}", request);
        ModelTag modelName = new ModelTag(request.model(), null);

        return tags
                .filter(t -> t != null && t.tagsResponse().models() != null && t.tagsResponse.models().contains(modelName))
                .flatMap(o -> {
                            log.info("{}/api/chat", o.url());
                            return webClientBuilder.baseUrl(o.url()).build()
                                    .post()
                                    .uri("/api/chat")
                                    .bodyValue(request)
                                    .accept(MediaType.APPLICATION_NDJSON)
                                    .exchangeToFlux(response -> response.bodyToFlux(ChatStreamResponse.class));
                        }

                );

    }

    private record MetaTagResponse(String url, TagsResponse tagsResponse) {
    }
}