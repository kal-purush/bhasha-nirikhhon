package scp;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientConfigInfo {
    @Value("${spring.application.name}")
    private String appName;

    @Value("${eureka.client.service-url.defaultZone}")
    private String eurekaServerName;

    @Value("${server.port}")
    private Integer port;

    @GetMapping("/configInfo")
    public String getConfigInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("appName=").append(appName)
        .append(", eurekaServerName=").append(eurekaServerName)
        .append(", port=").append(port);
        return sb.toString();
    }
}