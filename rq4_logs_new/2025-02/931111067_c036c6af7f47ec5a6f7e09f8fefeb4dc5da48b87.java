package client;

import exception.InvalidRequestException;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import ru.practicum.dto.EndpointHitDto;
import ru.practicum.dto.ViewStatsDto;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class RestStatClient implements StatClient {

    private final String statUrl;
    private final RestClient restClient;


    public RestStatClient(RestClient restClient, String statUrl) {
        this.statUrl = statUrl;
        this.restClient = RestClient.builder()
                .baseUrl(statUrl)
                .build();
    }

    @Override
    public void saveHit(EndpointHitDto endpointHitDto) {
        restClient.post()
                .uri("/hit")
                .contentType(MediaType.APPLICATION_JSON)
                .body(endpointHitDto)
                .retrieve()
                .onStatus(status -> status != HttpStatus.CREATED, (request, response) -> {
                    throw new InvalidRequestException(response.getStatusCode().value() + ": " + response.getBody());
                });
    }

    @Override
    public List<ViewStatsDto> getStats(String start, String end, List<String> uris, Boolean unique) {
        String encodeStart = URLEncoder.encode(start, StandardCharsets.UTF_8);
        String encodeEnd = URLEncoder.encode(end, StandardCharsets.UTF_8);
        return restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/stats")
                        .queryParam("start", encodeStart)
                        .queryParam("end", encodeEnd)
                        .queryParam("uris", uris)
                        .queryParam("unique", unique)
                        .build())
                .retrieve()
                .onStatus(status -> status != HttpStatus.OK, (request, response) -> {
                    throw new InvalidRequestException(response.getStatusCode().value() + ": " + response.getBody());
                })
                .body(ParameterizedTypeReference.forType(List.class));
    }
}