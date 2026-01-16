package com.sastry.spring.bootbatch.config;

import com.sastry.spring.bootbatch.tasklet.PersonTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class PersonJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    public PersonJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    Job ReadPersonJob() {
        return jobBuilderFactory.get("ReadPersonJob").incrementer(new RunIdIncrementer()).
                start(ReadPersonStep()).build();
    }

    @Bean
    Step ReadPersonStep() {
        return stepBuilderFactory.get("ReadPersonStep").tasklet(personTasklet()).build();
    }

    @Bean("personTasklet")
    public PersonTasklet personTasklet() {
        return new PersonTasklet();
    }


}