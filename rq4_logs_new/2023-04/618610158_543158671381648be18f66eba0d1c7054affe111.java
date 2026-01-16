package ru.practicum.stat.client.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.stat.client.dtos.EndpointHitDto;
import ru.practicum.stat.client.dtos.StatisticDto;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class StatClientImpl implements StatClient {
    private final RestTemplate restTemplate;

    @Value("${stat-server.url}")
    private String serviceUrl;

    public StatClientImpl(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    @Override
    public ResponseEntity<Object> hit(EndpointHitDto endpointHitDto) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<EndpointHitDto> request = new HttpEntity<>(endpointHitDto, headers);
        return restTemplate.exchange(
                serviceUrl + "/hit",
                HttpMethod.POST,
                request,
                Object.class
        );
    }

    @Override
    public List<StatisticDto> stats(LocalDateTime start, LocalDateTime end, List<String> uris, Boolean unique) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(serviceUrl + "/stats")
                .queryParam("start", start.toString())
                .queryParam("end", end.toString())
                .queryParam("unique", unique.toString());
        for (String uri: uris) {
            uriBuilder.queryParam("uris", uri);
        }
        ResponseEntity<List<StatisticDto>> responseEntity =
                restTemplate.exchange(
                        uriBuilder.toUriString(),
                        HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<>() {}
                );
        return responseEntity.getBody();
    }
}