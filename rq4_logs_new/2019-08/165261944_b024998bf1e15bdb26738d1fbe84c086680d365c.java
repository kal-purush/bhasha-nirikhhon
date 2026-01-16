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
import uk.gov.hmcts.reform.amlib.models.UserCaseRolesEnvelope;

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

public class ReturnCaseRolesForUserIntegrationTest extends PreconfiguredIntegrationBaseTest {

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
        idamRoleWithExplicitAccess = UUID.randomUUID().toString();

        importerService.addRole(idamRoleWithExplicitAccess, IDAM, PUBLIC, EXPLICIT);
        importerService.addResourceDefinition(resourceDefinition =
            createResourceDefinition(serviceName, "case", UUID.randomUUID().toString()));
    }

    @Test
    void whenUserHasNoAccessToCaseShouldReturnEmptyListOfRoles() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "some user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "other user", idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasNoReadAccessToCaseShouldReturnEmptyListOfRoles() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(DELETE, UPDATE)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasOnlyAttributeLevelAccessToCaseShouldReturnEmptyListOfRoles() {
        service.grantExplicitResourceAccess(
            createGrant(resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition,
                Collections.singletonMap(JsonPointer.valueOf("/some-attribute"), ImmutableSet.of(CREATE, READ))));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasAccessToCaseWithNullRelationshipShouldReturnEmptyListOfRoles() {
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, null, resourceDefinition, ImmutableSet.of(READ)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(ImmutableList.of())
            .build());
    }

    @Test
    void whenUserHasAccessToCaseWithSingleRelationshipShouldReturnListOfRolesWithSingleRole() {
        String idamRoleWithExplicitAccess1 = UUID.randomUUID().toString();
        importerService.addRole(idamRoleWithExplicitAccess1, IDAM, PUBLIC, EXPLICIT);

        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "other user", idamRoleWithExplicitAccess1, resourceDefinition, ImmutableSet.of(READ)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(ImmutableList.of(idamRoleWithExplicitAccess))
            .build());
    }

    @Test
    void whenUserHasAccessToCaseWithMultipleRelationshipsShouldReturnListOfRolesWithAllRoles() {
        String idamRoleWithExplicitAccess1 = UUID.randomUUID().toString();
        String idamRoleWithExplicitAccess2 = UUID.randomUUID().toString();
        importerService.addRole(idamRoleWithExplicitAccess1, IDAM, PUBLIC, EXPLICIT);
        importerService.addRole(idamRoleWithExplicitAccess2, IDAM, PUBLIC, EXPLICIT);

        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess1, resourceDefinition, ImmutableSet.of(READ, CREATE)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, "other user", idamRoleWithExplicitAccess2, resourceDefinition, ImmutableSet.of(READ)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(sortedImmutableListOf(idamRoleWithExplicitAccess, idamRoleWithExplicitAccess1))
            .build());
    }

    @Test
    void whenUserHasAccessToCaseWithMultipleRelationshipsIncludingNullShouldReturnListOfRolesWithAllRolesExceptNull() {
        String idamRoleWithExplicitAccess1 = UUID.randomUUID().toString();
        importerService.addRole(idamRoleWithExplicitAccess1, IDAM, PUBLIC, EXPLICIT);

        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess, resourceDefinition, ImmutableSet.of(READ)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, idamRoleWithExplicitAccess1, resourceDefinition, ImmutableSet.of(READ, CREATE)));
        service.grantExplicitResourceAccess(createGrantForWholeDocument(
            resourceId, accessorId, null, resourceDefinition, ImmutableSet.of(READ, DELETE)));

        UserCaseRolesEnvelope result = service.returnUserCaseRoles(resourceId, accessorId);

        assertThat(result).isEqualTo(UserCaseRolesEnvelope.builder()
            .caseId(resourceId)
            .userId(accessorId)
            .roles(sortedImmutableListOf(idamRoleWithExplicitAccess, idamRoleWithExplicitAccess1))
            .build());
    }

    private List<String> sortedImmutableListOf(String... roles) {
        return ImmutableList.sortedCopyOf(Arrays.asList(roles));
    }
}