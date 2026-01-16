package com.kafka.practise.twitterkafka;

import com.kafka.practise.twitterkafka.config.TwitterToKafkaServiceConfigData;
import com.kafka.practise.twitterkafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TwitterKafkaApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaApplication.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final StreamRunner runner;
    public static void main(String[] args) {
        logger.info("Twitter Kafka Application Started ..........");
        SpringApplication.run(TwitterKafkaApplication.class, args);
    }

    public TwitterKafkaApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, StreamRunner streamRunner) {
        this.configData = twitterToKafkaServiceConfigData;
        this.runner = streamRunner;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Twitter Kafka execution Started ..........");
        logger.info(Arrays.toString(configData.getTwitterKeywords().toArray(new String[]{})));
        runner.start();
        configData.getTwitterKeywords().forEach(System.out::println);
    }
}