package ru.practicum.stat_server.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.dto.EndpointHit;
import ru.practicum.stat_server.service.StatsService;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestController
@RequestMapping(path = "/hit")
public class HitController {

    private final StatsService statsService;

    @Autowired
    public HitController(StatsService statsService) {
        this.statsService = statsService;
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping
    public EndpointHit createHit(HttpServletRequest request, @RequestBody EndpointHit endpointHit) {
        log.info("Получен запрос к эндпоинту: '{} {}', Строка параметров запроса: '{}'",
                request.getMethod(), request.getRequestURI(), request.getQueryString());
        return statsService.createHit(request, endpointHit);
    }

}