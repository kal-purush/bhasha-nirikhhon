package com.intouristing.intouristing.controller.websocket;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.user.SimpSession;
import org.springframework.messaging.simp.user.SimpSubscription;
import org.springframework.messaging.simp.user.SimpUser;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

/**
 * Created by Marcelo Lacroix on 18/08/2019.
 */
@SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
@RestController
@Slf4j
public class RootWsController {

    @Autowired
    SimpMessageSendingOperations messagingTemplate;
    @Autowired
    private SimpUserRegistry simpUserRegistry;

    List<String> getUsers(String destination, String... sendToVarargs) {
        if (isNull(sendToVarargs)) {
            throw new RuntimeException("user.destination.required");
        }

        String subDestination = "/user/queue" + destination;
        Set<SimpSubscription> subscriptionsSet = simpUserRegistry.findSubscriptions((simpSubscription) -> simpSubscription.getDestination().equals(subDestination));
        SimpSubscription[] subscriptions = subscriptionsSet.toArray(new SimpSubscription[0]);
        List<String> userList = new ArrayList<>();
        List<String> sendTo = Arrays.stream(sendToVarargs).map(String::toLowerCase).collect(Collectors.toList());
        if (ArrayUtils.isNotEmpty(subscriptions)) {
            userList = Stream.of(subscriptionsSet.toArray(subscriptions))
                    .map(SimpSubscription::getSession)
                    .map(SimpSession::getUser)
                    .map(SimpUser::getName)
                    .map(String::toLowerCase)
                    .filter(sendTo::contains)
                    .collect(Collectors.toList());
        }

        return userList;
    }

    void sendToUser(String destination, Object message, String username) {
        messagingTemplate.convertAndSendToUser(
                username,
                "/queue/" + destination,
                message
        );
    }

    void send(String destination, Object message) {
        messagingTemplate.convertAndSend(destination, message);
    }

    void sendToAnotherUser(String destination, Object message, String sentBy, String... usernames) {
        for (String username : usernames) {
            messagingTemplate.convertAndSendToUser(
                    username,
                    "/queue/" + destination,
                    message,
                    Map.of("SentBy", sentBy)
            );
        }
    }

    void sendToAnotherUser(String destination, Object message, String sentBy, List<String> usernames) {
        this.sendToAnotherUser(
                destination,
                message,
                sentBy,
                usernames.toArray(new String[0])
        );
    }

}