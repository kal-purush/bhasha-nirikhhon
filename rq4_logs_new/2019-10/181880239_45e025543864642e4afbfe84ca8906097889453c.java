package ch.uzh.ifi.access.student.config;

import ch.uzh.ifi.access.student.evaluation.process.EvalMachineRepoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Configuration
@EnableScheduling
public class SchedulingConfig {

    private static Logger logger = LoggerFactory.getLogger(SchedulingConfig.class);

    private static final long FIXED_DELAY_IN_MINUTES = 5;

    private EvalMachineRepoService machineRepository;

    public SchedulingConfig(EvalMachineRepoService machineRepository) {
        this.machineRepository = machineRepository;
    }

    @Scheduled(fixedDelay = FIXED_DELAY_IN_MINUTES * 60000)
    public void cleanUpRepo() {
        Instant threshold = Instant.now().minus(5, ChronoUnit.MINUTES);
        logger.debug("Starting state machine cleanup. Repository size {}, removing machine older than {}", machineRepository.count(), threshold);
        machineRepository.removeMachinesOlderThan(threshold);
        logger.debug("Completed cleanup. Repository size {}", machineRepository.count());
    }
}