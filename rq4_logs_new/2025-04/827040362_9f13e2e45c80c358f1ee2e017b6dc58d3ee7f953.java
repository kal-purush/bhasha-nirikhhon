package org.iromu.ai.gateway;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.iromu.ai.model.ollama.TagsResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class TagDiscoveryService {

    private final List<String> urls;
    private final WebClient.Builder webClientBuilder;

    @Getter
    private Flux<MetaTagResponse> tags;

    public TagDiscoveryService(AiConfig aiConfig, WebClient.Builder webClientBuilder) {
        this.urls = aiConfig.getUrls();
        this.webClientBuilder = webClientBuilder;
    }

    @PostConstruct
    public void startDiscoveryLoop() {
        Flux.interval(Duration.ZERO, Duration.ofSeconds(10))
                .flatMap(tick -> discoverOnce())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private Flux<MetaTagResponse> discoverOnce() {
        this.tags = Flux.fromIterable(urls)
                .flatMap(url -> {
                    log.debug("Discovering tags from {}/api/tags", url);
                    return webClientBuilder.baseUrl(url).build()
                            .get()
                            .uri("/api/tags")
                            .accept(MediaType.APPLICATION_JSON)
                            .exchangeToMono(response -> response.bodyToMono(TagsResponse.class))
                            .onErrorReturn(new TagsResponse(new ArrayList<>()))
                            .map(tagsResponse -> new MetaTagResponse(url, tagsResponse));
                })
                .cache();

        return this.tags;
    }

    public record MetaTagResponse(String url, TagsResponse tagsResponse) {
    }
}