package dev.frilly.locket.component;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.FirebaseMessaging;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * The component that exports a Firebase bean after configuring.
 */
@Component
public final class FirebaseComponent {

  @Bean
  public FirebaseMessaging messaging() {
    try {
      final var input = new FileInputStream("./locket-firebase-key.json");
      final var options = FirebaseOptions.builder()
          .setCredentials(GoogleCredentials.fromStream(input))
          .build();

      final var app = FirebaseApp.initializeApp(options);
      return FirebaseMessaging.getInstance();
    } catch (IOException ex) {
      throw new NullPointerException("Failed to initialize Firebase Messaging");
    }
  }

}