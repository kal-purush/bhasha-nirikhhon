package AIWA.McpBackend.controller.api.dto.firewallpolicy;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FirewallPolicyRequestDto {
    private String policyName;          // 방화벽 정책 이름
    private String networkName;         // 네트워크 이름
    private String direction;           // 트래픽 방향 (INGRESS 또는 EGRESS)
    private String action;              // 액션 (ALLOW 또는 DENY)
    private String sourceRange;         // 소스 범위
    private String targetTag;           // 대상 태그 (선택 사항)
    private String protocol;            // 프로토콜 (예: tcp, udp)
    private int port;                   // 포트 번호

    // 기본 생성자
    public FirewallPolicyRequestDto() {}

    // 생성자
    public FirewallPolicyRequestDto(String policyName, String networkName, String direction,
                                    String action, String sourceRange, String targetTag,
                                    String protocol, int port) {
        this.policyName = policyName;
        this.networkName = networkName;
        this.direction = direction;
        this.action = action;
        this.sourceRange = sourceRange;
        this.targetTag = targetTag;
        this.protocol = protocol;
        this.port = port;
    }
}