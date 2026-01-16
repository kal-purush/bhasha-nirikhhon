package AIWA.McpBackend.controller.api.dto.staticip;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StaticIpRequestDto {
    private String ipName;        // IP 이름
    private String region;        // IP를 생성할 GCP 리전
    private String addressType;   // IP 유형 (EXTERNAL / INTERNAL)

    // 생성자 (이 객체를 생성할 때 사용할 수 있음)
    public StaticIpRequestDto(String ipName, String region, String addressType) {
        this.ipName = ipName;
        this.region = region;
        this.addressType = addressType;
    }

    // 기본 생성자
    public StaticIpRequestDto() {}
}