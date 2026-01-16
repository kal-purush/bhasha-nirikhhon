package integration.uk.gov.hmcts.reform.amlib;

import com.fasterxml.jackson.core.JsonPointer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import integration.uk.gov.hmcts.reform.amlib.base.PreconfiguredIntegrationBaseTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.hmcts.reform.amlib.AccessManagementService;
import uk.gov.hmcts.reform.amlib.DefaultRoleSetupImportService;
import uk.gov.hmcts.reform.amlib.models.ResourceDefinition;
import uk.gov.hmcts.reform.amlib.models.UserCasesEnvelope;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.hmcts.reform.amlib.enums.AccessType.EXPLICIT;
import static uk.gov.hmcts.reform.amlib.enums.Permission.CREATE;
import static uk.gov.hmcts.reform.amlib.enums.Permission.DELETE;
import static uk.gov.hmcts.reform.amlib.enums.Permission.READ;
import static uk.gov.hmcts.reform.amlib.enums.Permission.UPDATE;
import static uk.gov.hmcts.reform.amlib.enums.RoleType.IDAM;
import static uk.gov.hmcts.reform.amlib.enums.SecurityClassification.PUBLIC;
import static uk.gov.hmcts.reform.amlib.helpers.DefaultRoleSetupDataFactory.createResourceDefinition;
import static uk.gov.hmcts.reform.amlib.helpers.TestDataFactory.createGrant;
import static uk.gov.hmcts.reform.amlib.helpers.TestDataFactory.createGrantForWholeDocument;

public class ReturnUserCasesIntegrationTest extends PreconfiguredIntegrationBaseTest {

    private static AccessManagementService service = initService(AccessManagementService.class);
    private static DefaultRoleSetupImportService importerService = initService(DefaultRoleSetupImportService.class);

    private String resourceId;
    private String accessorId;
    private String idamRoleWithExplicitAccess;
    private ResourceDefinition resourceDefinition;

    @BeforeEach
    void setUp() {
        resourceId = UUID.randomUUID().toString();
        accessorId = UUID.randomUUID().toString();

        importerService.addRole(idamRoleWithExplicitAccess = UUID.randomUUID().toString(), IDAM, PUBLIC, EXPLICIT);
        importerService.addResourceDefinition(resourceDefinition =
            createResourceDefinition(serviceName, "case", UUID.randomUUID().toString()));
    }

    @Test
    void whenUserHasAccessToNoCasesShouldReturnEmptyList() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "some user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "other user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));

        UserCasesEnvelope result = service.returnUserCases(accessorId);

        assertThat(result).isEqualTo(UserCasesEnvelope.builder()
            .userId(accessorId)
            .cases(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasNoReadAccessToCaseShouldReturnEmptyList() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(CREATE, UPDATE)));

        UserCasesEnvelope result = service.returnUserCases(accessorId);

        assertThat(result).isEqualTo(UserCasesEnvelope.builder()
            .userId(accessorId)
            .cases(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasNoRootLevelReadAccessToCaseShouldReturnEmptyList() {
        service.grantExplicitResourceAccess(
            createGrant(resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition,
            Collections.singletonMap(JsonPointer.valueOf("/some-attribute"), ImmutableSet.of(CREATE, READ))));

        UserCasesEnvelope result = service.returnUserCases(accessorId);

        assertThat(result).isEqualTo(UserCasesEnvelope.builder()
            .userId(accessorId)
            .cases(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasAccessToOneCaseShouldReturnOneCase() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ, DELETE)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "other user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));

        UserCasesEnvelope result = service.returnUserCases(accessorId);

        assertThat(result).isEqualTo(UserCasesEnvelope.builder()
            .userId(accessorId)
            .cases(ImmutableList.of(resourceId))
            .build());
    }

    @Test
    void whenUserHasAccessToMoreThanOneCaseShouldReturnAllCases() {
        String resourceId1 = UUID.randomUUID().toString();
        String resourceId2 = UUID.randomUUID().toString();
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId1, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ, UPDATE)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId2, "other user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));

        UserCasesEnvelope result = service.returnUserCases(accessorId);

        assertThat(result).isEqualTo(UserCasesEnvelope.builder()
            .userId(accessorId)
            .cases(sortedImmutableListOf(resourceId, resourceId1))
            .build());
    }

    private List<String> sortedImmutableListOf(String... roles) {
        return ImmutableList.sortedCopyOf(Arrays.asList(roles));
    }
}