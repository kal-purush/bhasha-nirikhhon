package uk.gov.justice.hmpps.datacompliance.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;
import uk.gov.justice.hmpps.datacompliance.jobs.offenderdeletion.OffenderDeletionJob;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.quartz.CronScheduleBuilder.cronSchedule;

@Slf4j
@Getter
@Builder
@Validated
@Configuration
@NoArgsConstructor
@AllArgsConstructor
@ConditionalOnProperty(name = "offender.deletion.cron")
public class OffenderDeletionConfig {

    @Value("#{T(java.time.LocalDateTime).parse('${offender.deletion.initial.window.start}')}")
    private LocalDateTime initialWindowStart;

    @Value("${offender.deletion.window.length}")
    private Duration windowLength;

    @Bean
    public Trigger offenderDeletionTrigger(@Value("${offender.deletion.cron}") final String offenderDeletionCron) {

        log.info("Configured to delete offenders with schedule: '{}'", offenderDeletionCron);

        return TriggerBuilder.newTrigger().forJob(offenderDeletionJobDetails())
                .withIdentity("offender-deletion-trigger")
                .withSchedule(cronSchedule(offenderDeletionCron))
                .build();
    }

    @Bean
    public JobDetail offenderDeletionJobDetails() {
        return JobBuilder.newJob(OffenderDeletionJob.class)
                .withIdentity("offender-deletion-job")
                .storeDurably()
                .build();
    }
}