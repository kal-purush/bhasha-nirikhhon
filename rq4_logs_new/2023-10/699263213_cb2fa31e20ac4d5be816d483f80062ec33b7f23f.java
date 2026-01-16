package ru.practicum.ewm;

import lombok.RequiredArgsConstructor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class Client {
    private final RestTemplate rt;
    public static final String BASE_URI = "http://localhost:9090";

    public ResponseEntity<EndpointHitDto> saveHit(EndpointHitDto hit) {
        log.info("Отправлен запрос на сохранение информации о запросе к эндпоинту {}", hit);
        String uri = BASE_URI + "/hit";
        return rt.postForEntity(uri, hit, EndpointHitDto.class);
    }

    public ResponseEntity<List<ViewStats>> getStats(String start, String end, String[] uris, Boolean unique) {
        log.info("Отправлен запрос на получение статистики по посещениям");

        String uri = BASE_URI + "/stats";

        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(uri)
                .queryParam("start", "{start}")
                .queryParam("end", "{end}")
                .queryParam("uris", "{uris}")
                .queryParam("unique", "{unique}");

        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("start", start);
        uriVariables.put("end", end);
        uriVariables.put("uris", uris);
        uriVariables.put("unique", unique);

        String expandedUriString = builder.buildAndExpand(uriVariables).toUriString();
        ParameterizedTypeReference<List<ViewStats>> responseType = new ParameterizedTypeReference<>() {};

        return rt.exchange(expandedUriString, HttpMethod.GET, null, responseType);
    }
}