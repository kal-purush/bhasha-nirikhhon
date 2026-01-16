package com.roomfit.be.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskExecutorConfig {

    @Bean(name = "taskExecutor")
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10); // 최소 스레드 수
        taskExecutor.setMaxPoolSize(20); // 최대 스레드 수
        taskExecutor.setQueueCapacity(100); // 큐 크기
        taskExecutor.setThreadNamePrefix("async-"); // 스레드 이름 접두사
        taskExecutor.setKeepAliveSeconds(60); // 스레드의 유휴 시간 (초)
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true); // 종료 시 작업 완료 대기
        taskExecutor.setAwaitTerminationSeconds(30); // 작업 대기 종료 시간 (초)
        taskExecutor.initialize();
        return taskExecutor;
    }
}