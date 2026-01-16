package uk.gov.hmcts.reform.workallocation.ccd;

import org.apache.http.HttpHeaders;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@FeignClient(name = "ccd-api", url = "${ccd.baseUrl}")
public interface CcdClient {

    String SERVICE_AUTH_HEADER = "ServiceAuthorization";

    @RequestMapping(
        method = RequestMethod.POST,
        value = "/searchCases",
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    Map<String, Object> searchCases(
        @RequestHeader(HttpHeaders.AUTHORIZATION) final String authorisation,
        @RequestHeader(SERVICE_AUTH_HEADER) final String serviceAuthorisation,
        @RequestParam("ctid") final String ctid,
        @RequestBody final String body
    );
}