package com.SkillBox.users.controller;

import com.SkillBox.users.entity.Subscription;
import com.SkillBox.users.service.SubscriptionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/subscription")
public class SubscriptionController {

    private final SubscriptionService subscriptionService;

    @GetMapping("/{userId}")
    public List<Long> getUserSubscriptions(@PathVariable Long userId) {
        return subscriptionService.getUserSubscriptions(userId);
    }

    @PostMapping("/{userId1}/{userId2}")
    public Subscription createSubscription(@PathVariable Long userId1, @PathVariable Long userId2) {
        return subscriptionService.createSubscription(userId1, userId2);
    }

    @DeleteMapping("/{userId1}/{userId2}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteSubscription(@PathVariable Long userId1, @PathVariable Long userId2) {
        subscriptionService.deleteSubscription(userId1, userId2);
    }

}