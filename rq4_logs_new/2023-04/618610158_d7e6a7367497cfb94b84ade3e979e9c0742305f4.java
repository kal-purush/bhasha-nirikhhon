package ru.practicum.ewm.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.practicum.stat.client.client.StatClient;
import ru.practicum.stat.client.client.StatClientImpl;
import ru.practicum.stat.client.dtos.EndpointHitDto;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;

@Service
@Slf4j
public class StatisticService {

    @Value("${app.name}")
    private String appName;

    private final StatClient statClient;

    public StatisticService() {
        this.statClient = new StatClientImpl();
    }

    public void hit(HttpServletRequest httpRequest) {
        EndpointHitDto endpointHitDto = new EndpointHitDto();
        endpointHitDto.setApp(appName);
        endpointHitDto.setIp(httpRequest.getRemoteAddr());
        endpointHitDto.setUri(httpRequest.getRequestURI());
        endpointHitDto.setTimestamp(LocalDateTime.now());
        log.info("Sending hit {} from {}", httpRequest.getRequestURI(), httpRequest.getRemoteAddr());
        statClient.hit(endpointHitDto);
        log.info("Hit success");
    }
}