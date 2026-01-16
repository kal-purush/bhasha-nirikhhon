package AIWA.McpBackend.controller.api.restcontroller.cloudmonitoring;

import AIWA.McpBackend.controller.api.dto.cloudmonitoring.CloudMonitoringRequestDto;
import AIWA.McpBackend.controller.api.dto.response.CommonResult;
import AIWA.McpBackend.service.gcp.cloudmonitoring.CloudMonitoringService;
import AIWA.McpBackend.service.response.ResponseService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/gcp/api/cloud-monitoring")
@RequiredArgsConstructor
public class CloudMonitoringController {

    private final CloudMonitoringService cloudMonitoringService;

    private final ResponseService responseService;

    @PostMapping("/create-alarm")
    public CommonResult createCloudMonitoringAlarm(@RequestBody CloudMonitoringRequestDto alarmRequest, @RequestParam String userId) {
        try {
            cloudMonitoringService.createCloudMonitoringAlarm(alarmRequest, userId);
            return responseService.getSuccessResult();
        } catch (Exception e) {
            e.printStackTrace();
            return responseService.getFailResult("CloudMonitoring creation failed: " + e.getMessage());
        }
    }

    @DeleteMapping("/delete-alarm")
    public CommonResult deleteCloudMonitoringAlarm(@RequestParam String alarmName, @RequestParam String userId) {
        try {
            cloudMonitoringService.deleteCloudMonitoringAlarm(alarmName, userId);
            return responseService.getSuccessResult();
        } catch (Exception e) {
            e.printStackTrace();
            return responseService.getFailResult("CloudMonitoring deletion failed: " + e.getMessage());
        }
    }
}