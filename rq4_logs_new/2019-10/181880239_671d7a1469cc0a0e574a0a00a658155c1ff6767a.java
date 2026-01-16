package ch.uzh.ifi.access;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
public class ServerInfoController {

    private final ServerInfo serverInfo;

    public ServerInfoController(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @GetMapping("/info")
    public ResponseEntity<?> info() {
        Map<String, String> response = new HashMap<>();
        response.put("time", ZonedDateTime.now().toOffsetDateTime().toString());

        if (serverInfo != null) {
            response.put("version", serverInfo.version);
        }
        return ResponseEntity.ok(response);
    }

    @Component
    @Data
    @Configuration
    @ConfigurationProperties(prefix = "server.info")
    static class ServerInfo {
        private String version;
    }
}