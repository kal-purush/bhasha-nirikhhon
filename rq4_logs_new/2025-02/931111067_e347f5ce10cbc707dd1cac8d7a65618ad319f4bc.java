package ru.practicum.stat_server.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.dto.EndpointHitDto;
import ru.practicum.dto.ViewStatsDto;
import ru.practicum.stat_server.service.StatService;
import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
public class StatController {
    private final StatService statService;

    @GetMapping("/stats")
    public List<ViewStatsDto> getStats(@RequestParam String start,
                                       @RequestParam String end,
                                       @RequestParam(required = false) List<String> uris,
                                       @RequestParam(required = false, defaultValue = "false") Boolean unique) {
        log.info("Getting stats from {} to {} - Started", start, end);
        List<ViewStatsDto> stats = statService.getStats(start, end, uris, unique);
        log.info("Getting stats from {} to {} - Finished", start, end);
        return stats;
    }

    @PostMapping("/hit")
    @ResponseStatus(HttpStatus.CREATED)
    public void saveHit(@RequestBody EndpointHitDto endpointHitDto) {
        log.info("Saving hit {} - Started", endpointHitDto);
        statService.saveHit(endpointHitDto);
        log.info("Saving hit {} - Finished", endpointHitDto);
    }
}