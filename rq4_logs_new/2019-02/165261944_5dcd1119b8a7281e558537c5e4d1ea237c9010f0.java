package uk.gov.hmcts.reform.amlib.models;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@SuppressWarnings("PMD")
public abstract class AbstractAccessMetadata {

    private final String resourceId;
    private final String accessorId;
    private final String accessType;
    private final String serviceName;
    private final String resourceType;
    private final String resourceName;
    private final String attribute;
    private final String securityClassification;

}