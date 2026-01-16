package dev.orion.api;

import dev.orion.api.endpoint.UserEndpoint;
import dev.orion.api.endpoint.body.ConnectUserResponseBody;
import dev.orion.api.endpoint.body.DefaultErrorResponseBody;
import dev.orion.client.UserClient;
import dev.orion.client.dto.UserClientResponse;
import dev.orion.entity.User;
import dev.orion.fixture.UserFixture;
import dev.orion.services.interfaces.UserService;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.quarkus.test.junit.mockito.InjectSpy;
import lombok.val;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.hibernate.Session;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.MessageFormat;
import java.util.UUID;

import static io.restassured.RestAssured.given;

@QuarkusTest
@TestHTTPEndpoint(UserEndpoint.class)
public class UserEndpointTest {
    @Inject
    UserService testThis;
    @InjectMock
    @RestClient
    UserClient userClient;
    @InjectMock
    Session session;

    @InjectSpy
    UserService userService;

    private User user;

    private final String commonUserExternalId = UUID.randomUUID().toString();

    @BeforeEach
    public void setup() {
        Mockito.doNothing().when(session).persist(Mockito.any());
        UserFixture.mockUserClient(userClient, commonUserExternalId);
        user = UserFixture.mockFindUserByExternalId(commonUserExternalId);
    }

    @Test
    @DisplayName("[/{externalId}/connect - PATCH] Should connect the user and return the body")
    public void testConnectUser() {
        val responseBody = requestToConnectUser(commonUserExternalId, Response.Status.OK.getStatusCode(), ConnectUserResponseBody.class);

        Assertions.assertEquals(commonUserExternalId, responseBody.getUserExternalId());
        BDDMockito.then(userService).should().connectUser(commonUserExternalId);
    }

    @Test
    @DisplayName("[/{externalId}/connect - PATCH] Should not let connect user when it's no in a Activity")
    public void testTryConnectUserWithoutActivity() {
        user.activity = null;
        val responseBody = requestToConnectUser(commonUserExternalId, Response.Status.BAD_REQUEST.getStatusCode(), DefaultErrorResponseBody.class);

        val expectedMessage = MessageFormat.format("The user {0} must be in activity to be connected", commonUserExternalId);
        Assertions.assertEquals(expectedMessage, responseBody.getErrors().get(0));
        BDDMockito.then(userService).should().connectUser(commonUserExternalId);
    }

    private static <T> T requestToConnectUser(String commonUserExternalId, int statusCode, Class<T> responseClass) {
        return given()
                .pathParam("externalId", commonUserExternalId)
                .when()
                .patch("/{externalId}/connect")
                .then()
                .statusCode(statusCode)
                .extract()
                .body()
                .as(responseClass);
    }
}