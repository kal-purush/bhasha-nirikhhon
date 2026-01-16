package uk.gov.hmcts.reform.roleassignmentrefresh.domain.service.common;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import uk.gov.hmcts.reform.roleassignmentrefresh.feignclients.configuration.FeignClientConfiguration;
import uk.gov.hmcts.reform.roleassignmentrefresh.feignclients.configuration.FeignClientInterceptor;
import uk.gov.hmcts.reform.roleassignmentrefresh.feignclients.configuration.RASFeignClientFallback;

@FeignClient(value = "rasClient", url = "${feign.client.config.rasClient.url}",
        configuration = {FeignClientConfiguration.class, FeignClientInterceptor.class},
        fallback = RASFeignClientFallback.class)
public interface RASFeignClient {

    @GetMapping(value = "/am/role-assignments/user-count")
    ResponseEntity<Object> sendGetUserCountToRoleAssignmentService();
}