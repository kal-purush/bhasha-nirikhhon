package uk.gov.hmcts.reform.idam.bdd;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import uk.gov.hmcts.reform.idam.util.Constants;

public class RoleAssignmentStubs {
    public final WireMockServer wiremock = WireMockInstantiator.getWireMockInstance();

    public void setupRoleAssignmentPagedStub() {
        for (int i = 0; i < 3; i++) {
            wiremock.stubFor(
                WireMock.post(WireMock.urlPathEqualTo(Constants.ROLE_ASSIGNMENTS_QUERY_PATH))
                    .withHeader("pageNumber", WireMock.equalTo(String.valueOf(i)))
                    .willReturn(
                        WireMock.aResponse()
                            .withBodyFile("roleAssignmentsResponsePage" + i + ".json")
                            .withHeader("Content-Type", Constants.ROLE_ASSIGNMENTS_CONTENT_TYPE)
                    )
            );
        }
    }
}