package pl.dchruscinski.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;

import java.net.Socket;
import java.time.LocalDateTime;

@Component
public class CustomInfoContributor implements InfoContributor, HealthIndicator {
    public static final Logger logger = LoggerFactory.getLogger(CustomInfoContributor.class);
    public static final String HOST = "localhost";
    public static final int PORT = 8080;

    @Override
    public void contribute(Info.Builder builder) {
        LocalDateTime currentTime = LocalDateTime.now();
        builder.withDetail("currentTime", currentTime);

        builder.withDetail("health", getHealth(true));
    }

    @Override
    public Health health() {
        try (Socket socket = new Socket(HOST, PORT)) {
            if (socket.isConnected()) {
                logger.info("health(): already connected to: {}:{}", HOST, PORT);
                return Health.up().build();
            } else {
                logger.info("health(): not connected to: {}:{}", HOST, PORT);
                return Health.down().build();
            }
        } catch (Exception e) {
            logger.warn("health(): failed to connect to: {}:{}", HOST, PORT);
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}