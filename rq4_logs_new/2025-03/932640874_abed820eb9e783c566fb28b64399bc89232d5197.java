package dev.frilly.locket.controller;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.MulticastMessage;
import com.google.firebase.messaging.Notification;
import dev.frilly.locket.Constants;
import dev.frilly.locket.data.DeviceToken;
import dev.frilly.locket.data.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.format.DateTimeFormatter;

@RestController
public final class TestController {

  @Autowired
  private FirebaseMessaging messaging;

  @GetMapping("/test")
  public void getTest() {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var testMessage = MulticastMessage.builder()
        .addAllTokens(user.tokens().stream().map(DeviceToken::token).toList())
        .putData("id", String.valueOf(user.id()))
        .putData("username", user.username())
        .putData("email", user.email())
        .putData("avatar", user.avatarUrl())
        .putData("birthdate",
            user.birthdate().format(DateTimeFormatter.ISO_DATE_TIME))
        .setNotification(Notification.builder()
            .setImage(Constants.APP_IMAGE)
            .setTitle("Test Notification")
            .setBody("Notification body requested by " + user.username())
            .build())
        .build();

    messaging.sendEachForMulticastAsync(testMessage);
  }

}