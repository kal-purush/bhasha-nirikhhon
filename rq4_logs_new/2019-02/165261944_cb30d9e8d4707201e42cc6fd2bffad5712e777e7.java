package uk.gov.hmcts.reform.amapi.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.gov.hmcts.reform.amlib.AccessManagementService;
import uk.gov.hmcts.reform.amlib.enums.Permissions;
import uk.gov.hmcts.reform.amlib.models.ExplicitPermissions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Default endpoints per application.
 */
@RestController
@RequestMapping("lib")
public class AmLibProxyController {

    @Autowired private ObjectMapper mapper;
    @Autowired private AccessManagementService am;

    @SuppressWarnings("unchecked") // supressing compiler warning about casting from Object to List<String>
    @PostMapping("/create-resource-access")
    public void createResourceAccess(@RequestBody Map<String, Object> amData) {
        LinkedHashMap<String, List> rawExplicitPermissions = (LinkedHashMap) amData.get("explicitPermissions");
        List<String> userPermissions = rawExplicitPermissions.get("userPermissions");
        Permissions[] permissions = userPermissions.stream()
                .map(ep -> Permissions.valueOf(ep))
                .toArray(Permissions[]::new);

        ExplicitPermissions explicitPermissions = new ExplicitPermissions(permissions);

        am.createResourceAccess(amData.get("resourceId").toString(),
                amData.get("accessorId").toString(),
                explicitPermissions);
    }

    @PostMapping("/get-accessors-list")
    public List<String> getAccessorsList(@RequestBody Map<String, Object> amData) {
        return am.getAccessorsList(amData.get("userId").toString(), amData.get("resourceId").toString());
    }

    @PostMapping("/filter-resource")
    public JsonNode filterResource(@RequestBody Map<String, Object> amData) {
        JsonNode jsonNode = mapper.valueToTree(amData.get("resourceJson"));
        return am.filterResource(
            amData.get("userId").toString(),
            amData.get("resourceId").toString(),
            jsonNode
        );
    }
}