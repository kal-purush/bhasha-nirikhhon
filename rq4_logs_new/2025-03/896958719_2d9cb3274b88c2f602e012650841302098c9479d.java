package ru.otus.hw.commands;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import static ru.otus.hw.config.JobConfig.MILLIS_PARAM_NAME;

@RequiredArgsConstructor
@ShellComponent
public class BatchCommands {

    private final Job importFromDatabaseJob;

    private final JobLauncher jobLauncher;

    @ShellMethod(value = "startMigrationAuthorLauncher", key = "SMA")
    public void startMigrationAuthorJob() throws Exception {
        JobExecution execution = jobLauncher.run(importFromDatabaseJob, new JobParametersBuilder()
                .addLong(MILLIS_PARAM_NAME, System.currentTimeMillis())
                .toJobParameters());
        System.out.println(execution);
    }
}