package local.socialnetwork.authserver.listener;

import local.socialnetwork.authserver.event.UserDetailsEvent;

import local.socialnetwork.authserver.kafka.constant.KafkaTopics;

import local.socialnetwork.authserver.kafka.saga.signup.producer.user.UserDetailsCreationProducer;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import lombok.experimental.FieldDefaults;

import org.springframework.stereotype.Component;

import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class UserDetailsListener {

    final UserDetailsCreationProducer userDetailsCreationProducer;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleUserDetailsEvent(UserDetailsEvent event) {
        userDetailsCreationProducer.sendUserDetailsToCreate(KafkaTopics.AUTH_USER_REGISTERED_TOPIC,
                event.signUp(), event.authUserId());
    }
}