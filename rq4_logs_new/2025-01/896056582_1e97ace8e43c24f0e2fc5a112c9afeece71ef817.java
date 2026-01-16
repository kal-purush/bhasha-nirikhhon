package PlanQ.PlanQ.config;

import PlanQ.PlanQ.routine.RoutineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SchedulerConfig {
    private final RoutineService routineService;

    @Scheduled(cron = "0 0 0 * * *")
    public void printDate () {
        routineService.resetRoutine();
        log.info("루틴 초기화");
    }
}