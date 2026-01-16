package AIWA.McpBackend.controller.api.dto.cloudnat;

import lombok.Getter;

@Getter
public class CloudNatDto {
    private String gatewayName;
    private String network;
    private String region;
    private String natType;
    private String cloudRouter;
    private String status;

    public CloudNatDto(String gatewayName, String network, String region, String natType, String cloudRouter, String status) {
        this.gatewayName = gatewayName;
        this.network = network;
        this.region = region;
        this.natType = natType;
        this.cloudRouter = cloudRouter;
        this.status = status;
    }

    // Getters and setters
}