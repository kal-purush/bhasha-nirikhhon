package ru.volsu.coursebot.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import ru.volsu.coursebot.service.CacheService;

@Configuration
@EnableScheduling
public class ScheduledTask {

    @Autowired
    private CacheService cacheService;

    @Scheduled(fixedRate = 6000000)
    public void clearByTagCache() {
        cacheService.clearByTagCache();
    }

    @Scheduled(fixedRate = 6000000)
    public void clearByTitle() {
        cacheService.clearByTitle();
    }

    @Scheduled(fixedRate = 6000000)
    public void clearByFullText() {
        cacheService.clearByFullText();
    }
}