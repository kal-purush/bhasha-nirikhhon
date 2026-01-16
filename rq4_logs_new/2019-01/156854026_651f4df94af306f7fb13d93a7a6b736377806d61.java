package com.siatsenko.movieland.service.impl;

import com.siatsenko.movieland.service.SlowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DefaultSlowService implements SlowService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${slowService.pause:5000}")
    private int pauseMillis;

    @Override
    public void slow(long millis) {
        logger.debug("SlowService({}): start", millis);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.debug("SlowService: InterruptedException {}", e);
        }
        logger.debug("SlowService: stop");
    }

    @Override
    public void slow() {
        slow(pauseMillis);
    }
}