package local.socialnetwork.userservice.listener;

import local.socialnetwork.userservice.event.ProfileCompletedEvent;

import local.socialnetwork.userservice.kafka.constant.KafkaTopics;

import local.socialnetwork.userservice.kafka.saga.signup.producer.profile.ProfileRegistrationCompletedProducer;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import lombok.experimental.FieldDefaults;

import org.springframework.stereotype.Component;

import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ProfileCompletedListener {

    final ProfileRegistrationCompletedProducer profileRegistrationCompletedProducer;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleProfileCompletedEvent(ProfileCompletedEvent event) {
        profileRegistrationCompletedProducer.sendProfileDataToCreate(KafkaTopics.PROFILE_COMPLETED_TOPIC,
                event.user().getAuthUserId(), event.user().getId());
    }
}