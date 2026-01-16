package local.socialnetwork.userservice.kafka.saga.signup.consumer.profile;

import local.socialnetwork.userservice.kafka.constant.KafkaTopics;

import local.socialnetwork.userservice.service.UserService;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import lombok.experimental.FieldDefaults;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.Optional;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.CompletableFuture.runAsync;

@Slf4j
@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ProfileRegistrationCompletedFailedConsumer {

    static final String USER_DEFAULT_GROUP_ID = "user-default-group-id";

    static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    final UserService userService;

    @KafkaListener(topics = KafkaTopics.PROFILE_COMPLETED_FAILED_TOPIC, groupId = USER_DEFAULT_GROUP_ID)
    public void onProfileCompletedFailed(ConsumerRecord<String, String> consumerRecord) {

        runAsync(() -> {

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted during sleep", e);
            }

            parseAuthUserId(consumerRecord)
                    .ifPresentOrElse(
                            userService::deleteByAuthUserId,
                            () -> log.error("Failed to process consumer record due to invalid UUID: '{}'", consumerRecord.value())
                    );

        }, executorService);
    }

    private Optional<UUID> parseAuthUserId(ConsumerRecord<String, String> consumerRecord) {
        try {
            UUID authUserId = UUID.fromString(consumerRecord.value());
            return Optional.of(authUserId);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse UUID from message. Invalid data received: '{}'", consumerRecord.value(), e);
            return Optional.empty();
        }
    }
}