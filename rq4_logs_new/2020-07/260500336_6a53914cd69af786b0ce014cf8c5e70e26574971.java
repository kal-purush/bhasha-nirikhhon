package world.podo.emergency.infrastructure.firebase;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;

@Configuration
public class FirebaseConfig {
    @PostConstruct
    public void init() throws IOException {
        boolean existsDefaultApp = FirebaseApp.getApps()
                .stream()
                .map(FirebaseApp::getName)
                .anyMatch(FirebaseApp.DEFAULT_APP_NAME::equals);
        if (existsDefaultApp) {
            return;
        }
        Resource resource = new ClassPathResource("emergency-aacad-6cd9330f1240.json");
        try (InputStream inputStream = resource.getInputStream()) {
            FirebaseOptions options = new FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.fromStream(inputStream))
                    .build();
            FirebaseApp.initializeApp(options);
        }
    }
}