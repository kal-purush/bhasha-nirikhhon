package AIWA.McpBackend.controller.api.dto.vm;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VmRequestDto {
    private String vmName;           // VM 이름
    private String machineType;      // 머신 유형 (예: n1-standard-1)
    private String zone;             // GCP 존 (예: asia-northeast3-a)
    private String imageFamily;      // OS 이미지 패밀리 (예: debian-9)
    private String imageProject;     // OS 이미지 프로젝트 (예: debian-cloud)
    private String networkName;      // 네트워크 이름
    private String subnetworkName;   // 서브넷 이름
    private String diskType;         // 디스크 유형 (예: pd-standard)
    private String diskSizeGb;       // 디스크 크기 (GB)

    // 기본 생성자, 생성자 등을 추가 가능
    public VmRequestDto() {}
}