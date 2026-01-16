package AIWA.McpBackend.controller.api.dto.cloudmonitoring;

import lombok.Data;

@Data
public class CloudMonitoringRequestDto {
    private String displayName;          // 알람 이름
    private int columns;         // 모니터링할 메트릭 이름 (예: CPUUtilization)
    private String computeCpuChartTitle;          // 메트릭 네임스페이스 (예: AWS/EC2)
    private String computeMemoryChartTitle;          // 통계 유형 (예: Average, Sum)
    private String gkeCpuChartTitle; // 비교 연산자 (예: GreaterThanThreshold)
    private String cloudSqlCpuChartTitle;          // 임계값
    private String cloudSqlDiskChartTitle;     // 평가 주기 (메트릭을 평가할 기간)
    private String cloudStorageRequestCountChartTitle;             // 평가할 주기 (예: 300초)
    private String cloudStorageTotalBytesChartTitle;         // 알람을 적용할 EC2 인스턴스 ID
}