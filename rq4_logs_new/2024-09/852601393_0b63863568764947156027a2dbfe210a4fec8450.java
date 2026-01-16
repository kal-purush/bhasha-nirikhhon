package com.boot.Config;



import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.boot.Service.BoxOfficeService;
import com.boot.Service.MovieService;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    
    @Bean
    public Job movieJob(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("movieJob")
                .start(step)
                .build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, Tasklet tasklet) {
        return stepBuilderFactory.get("step")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Tasklet tasklet(MovieService movieService, BoxOfficeService boxOfficeService) {
        return (contribution, chunkContext) -> {
            movieService.deleteMovie();  // 영화 데이터 삭제
            boxOfficeService.deleteBoxOffice();  // 박스오피스 데이터 삭제        	
            movieService.insertMovie();  // 영화 데이터 수집
            boxOfficeService.insertBoxOffice();  // 박스오피스 데이터 수집
            return RepeatStatus.FINISHED;
        };
    }
}