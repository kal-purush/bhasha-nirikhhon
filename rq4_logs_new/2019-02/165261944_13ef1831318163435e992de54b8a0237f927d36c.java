package uk.gov.hmcts.reform.amlib.models;

import lombok.Builder;
import lombok.Data;
import uk.gov.hmcts.reform.amlib.enums.Permissions;

import java.util.Set;

@Data
@Builder
public class ExplicitAccessRecord {

    private final String resourceId;
    private final String accessorId;
    private final Set<Permissions> explicitPermissions;
    private final String accessType;
    private final String serviceName;
    private final String resourceType;
    private final String resourceName;
    private final String attribute;
    private final String securityClassification;

    @SuppressWarnings("squid:S00107") // Having so many arguments seems reasonable solution here
    public ExplicitAccessRecord(String resourceId,
                                String accessorId,
                                Set<Permissions> explicitPermissions,
                                String accessType,
                                String serviceName,
                                String resourceType,
                                String resourceName,
                                String attribute,
                                String securityClassification) {
        this.resourceId = resourceId;
        this.accessorId = accessorId;
        this.explicitPermissions = explicitPermissions;
        this.accessType = accessType;
        this.serviceName = serviceName;
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.attribute = attribute;
        this.securityClassification = securityClassification;
    }

    public int getPermissions() {
        return Permissions.sumOf(explicitPermissions);
    }
}