package com.SkillBox.users;

import com.SkillBox.users.entity.Subscription;
import com.SkillBox.users.repository.SubscriptionRepository;
import com.SkillBox.users.service.SubscriptionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@ExtendWith(SpringExtension.class)
public class SubscriptionServiceTest {

    @Autowired
    private SubscriptionService subscriptionService;
    @MockBean
    private SubscriptionRepository subscriptionRepository;

    @Test
    void createSubscription_shouldReturnExistingSubscription_whenAlreadyExists() {
        // given
        Long userId1 = 1L;
        Long userId2 = 2L;
        Subscription existingSubscription = new Subscription(1L, userId1, userId2);
        when(subscriptionRepository.findSubscriptionByUserId1AndUserId2(userId1, userId2))
                .thenReturn(Optional.of(existingSubscription));

        // when
        Subscription result = subscriptionService.createSubscription(userId1, userId2);

        // then
        assertEquals(existingSubscription, result);
        verify(subscriptionRepository, times(1)).findSubscriptionByUserId1AndUserId2(userId1, userId2);
        verify(subscriptionRepository, never()).save(any(Subscription.class));
    }

    @Test
    void createSubscription_shouldCreateNewSubscription_whenNotExists() {
        // given
        Long userId1 = 1L;
        Long userId2 = 2L;
        Subscription newSubscription = new Subscription(1L, userId1, userId2);
        when(subscriptionRepository.findSubscriptionByUserId1AndUserId2(userId1, userId2))
                .thenReturn(Optional.empty());
        when(subscriptionRepository.save(any(Subscription.class)))
                .thenReturn(newSubscription);

        // when
        Subscription result = subscriptionService.createSubscription(userId1, userId2);

        // then
        assertEquals(newSubscription, result);
        verify(subscriptionRepository, times(1)).findSubscriptionByUserId1AndUserId2(userId1, userId2);
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    @Test
    void getUserSubscriptions_shouldReturnUserSubscriptions() {
        // given
        Long userId = 1L;
        List<Subscription> subscriptions = new ArrayList<>();
        subscriptions.add(new Subscription(1L, userId, 2L));
        subscriptions.add(new Subscription(2L, userId, 3L));
        when(subscriptionRepository.findByUserId1(userId)).thenReturn(subscriptions);

        // when
        List<Long> result = subscriptionService.getUserSubscriptions(userId);

        // then
        assertEquals(2, result.size());
        assertEquals(List.of(2L, 3L), result);
        verify(subscriptionRepository, times(1)).findByUserId1(userId);
    }

    @Test
    void deleteSubscription_shouldDeleteExistingSubscription() {
        // given
        Long userId1 = 1L;
        Long userId2 = 2L;
        Subscription existingSubscription = new Subscription(1L, userId1, userId2);
        when(subscriptionRepository.findSubscriptionByUserId1AndUserId2(userId1, userId2))
                .thenReturn(Optional.of(existingSubscription));

        // when
        subscriptionService.deleteSubscription(userId1, userId2);

        // then
        verify(subscriptionRepository, times(1)).findSubscriptionByUserId1AndUserId2(userId1, userId2);
        verify(subscriptionRepository, times(1)).delete(existingSubscription);
    }
}