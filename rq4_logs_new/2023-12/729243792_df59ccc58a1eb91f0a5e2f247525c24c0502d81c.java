package com.chwipoClova.recruit.schedule;

import com.chwipoClova.recruit.service.RecruitService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RecruitScheduleTask {
    private final RecruitService recruitService;

    @Scheduled(cron = "0 55 0 * * ?")
    public void runTask() throws Exception {
        log.info("recruitService Scheduled start");
        recruitService.deleteBeforeRecruit();
        log.info("recruitService Scheduled end");
    }
}