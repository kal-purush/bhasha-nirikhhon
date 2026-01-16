package uk.gov.hmcts.reform.amlib.models;

import com.fasterxml.jackson.core.JsonPointer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import uk.gov.hmcts.reform.amlib.enums.Permission;

import java.util.Map;
import java.util.Set;

@Data
@Builder
@AllArgsConstructor
public class ExplicitAccessGrant {

    private final String resourceId;
    private final String accessorId;
    private final String accessType;
    private final String serviceName;
    private final String resourceType;
    private final String resourceName;
    private final Map<JsonPointer, Set<Permission>> attributePermissions;
    private final String securityClassification;
}