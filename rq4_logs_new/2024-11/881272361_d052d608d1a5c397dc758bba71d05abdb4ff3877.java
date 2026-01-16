package AIWA.McpBackend.controller.api.dto.staticip;

import lombok.Getter;

@Getter
public class StaticIpDto {
    private String name;
    private String address;
    private String accessType;  // "EXTERNAL" or "INTERNAL"
    private String region;
    private String type;  // 고정 IP의 상태 (e.g., "RESERVED")
    private String subnetwork;
    private String vpcNetwork;
    private String usedBy;  // IP가 사용 중인 리소스 (e.g., "instance-xyz")

    // 기본 생성자
    public StaticIpDto(String name, String address, String accessType, String region, String type,
                            String subnetwork, String vpcNetwork, String usedBy) {
        this.name = name;
        this.address = address;
        this.accessType = accessType;
        this.region = region;
        this.type = type;
        this.subnetwork = subnetwork;
        this.vpcNetwork = vpcNetwork;
        this.usedBy = usedBy;
    }
}