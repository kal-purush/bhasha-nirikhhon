package AIWA.McpBackend.controller.api.dto.cloudrouter;

import lombok.Getter;

@Getter
public class CloudRouterRequestDto {
    private String routerName;  // Cloud Router 이름
    private String region;      // GCP 리전
    private String network;     // 연결할 VPC 네트워크 이름
    private String description; // Cloud Router 설명 (필요한 경우 추가)

    public CloudRouterRequestDto(String routerName, String region, String network, String description) {
        this.routerName = routerName;
        this.region = region;
        this.network = network;
        this.description = description;
    }
}