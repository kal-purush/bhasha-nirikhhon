package AIWA.McpBackend.controller.api.dto.securitygroup;

import lombok.Getter;

@Getter
public class FireWallPolicyDto {
    private String name;
    private String direction;
    private String target;
    private String sourceRanges;
    private String protocolPorts;
    private boolean logEnabled;
    private int priority;
    private String network;

    public FireWallPolicyDto(String name, String direction, String target, String sourceRanges, String protocolPorts, boolean logEnabled, int priority, String network) {
        this.name = name;
        this.direction = direction;
        this.target = target;
        this.sourceRanges = sourceRanges;
        this.protocolPorts = protocolPorts;
        this.logEnabled = logEnabled;
        this.priority = priority;
        this.network = network;
    }

    // Getter and Setter 생략
}
