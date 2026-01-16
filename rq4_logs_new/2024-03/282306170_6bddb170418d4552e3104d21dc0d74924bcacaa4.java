package local.socialnetwork.profileservice.kafka.saga.signup.consumer.profile;

import com.fasterxml.jackson.databind.ObjectMapper;

import local.socialnetwork.profileservice.kafka.constant.KafkaTopics;

import local.socialnetwork.profileservice.dto.user.UserProfileDto;

import local.socialnetwork.profileservice.service.ProfileService;

import lombok.AccessLevel;

import lombok.RequiredArgsConstructor;

import lombok.experimental.FieldDefaults;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;

import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ProfileRegistrationCompletedConsumer {

    static final String PROFILE_GROUP_ID = "profile-default-group-id";

    final ProfileService profileService;
    final KafkaTemplate<String, String> kafkaTemplate;
    final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.PROFILE_COMPLETED_TOPIC, groupId = PROFILE_GROUP_ID)
    public void receiveProfileDataToCreate(ConsumerRecord<String, String> consumerRecord) {
        log.info("Received profile data to create. Topic: {} - Timestamp: {}", consumerRecord.topic(), consumerRecord.timestamp());
         parseUserProfileDto(consumerRecord)
                 .ifPresentOrElse(
                         this::createProfile,
                         ()-> log.error("Failed to process consumer record due to parsing error: '{}'", consumerRecord.value())
                 );
    }
    private Optional<UserProfileDto> parseUserProfileDto(ConsumerRecord<String, String> consumerRecord) {
        try {
            UserProfileDto userProfileDto = objectMapper.readValue(consumerRecord.value(), UserProfileDto.class);
            return Optional.ofNullable(userProfileDto);
        } catch (Exception e) {
            log.error("Failed to parse consumer record value. Error: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private void createProfile(UserProfileDto userProfileDto) {
        try {
            log.info("Attempting to save a profile");
            profileService.save(userProfileDto.profile());
        } catch (Exception e) {
            handleProfileCompletedFailed(userProfileDto, e);
        }
    }

    private void handleProfileCompletedFailed(UserProfileDto userProfileDto, Exception e) {
        log.error("Failed to save a profile. Error: {}", e.getMessage());
        kafkaTemplate.send(KafkaTopics.PROFILE_COMPLETED_FAILED_TOPIC, String.valueOf(userProfileDto.authUserId()));
    }
}