package edu.java.scrapper.clients;

import com.github.tomakehurst.wiremock.WireMockServer;
import edu.java.clients.GitHubClient;
import edu.java.dto.github.GitHubDTO;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertThrows;

public class GitHubClientTest {
    private static WireMockServer wireMockServer;
    private static String baseUrl;

    @BeforeAll
    public static void setUp() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();
        baseUrl = "http://localhost:" + wireMockServer.port();
    }

    @AfterAll
    public static void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testFetchRepo() {
        GitHubClient gitHubClient = new GitHubClient(baseUrl);

        String owner = "testOwner";
        String repo = "testRepo";
        String fullName = "testOwner/testRepo";
        String pushedAt = "2022-01-01T12:00:00Z";
        String responseBody = String.format(
            "{\"owner\": {\"login\": \"%s\", \"id\": 123}, \"full_name\": \"%s\", \"pushed_at\": \"%s\"}",
            owner,
            fullName,
            pushedAt
        );

        wireMockServer.stubFor(get(urlEqualTo("/repos/" + owner + "/" + repo))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(responseBody)));

        GitHubDTO gitHubDTO = gitHubClient.fetchRepo(owner, repo);

        Assertions.assertEquals(owner, gitHubDTO.owner().login());
        Assertions.assertEquals(fullName, gitHubDTO.fullName());
        Assertions.assertEquals(OffsetDateTime.parse(pushedAt), gitHubDTO.pushedAt());
    }

    @Test
    public void testFetchRepo_NotFound() {
        GitHubClient gitHubClient = new GitHubClient(baseUrl);

        String owner = "testOwnerXXX";
        String repo = "testRepoXXX";

        stubFor(get(urlEqualTo("/repos/" + owner + "/" + repo))
            .willReturn(aResponse()
                .withStatus(404)));

        assertThrows(WebClientResponseException.class, () -> gitHubClient.fetchRepo(owner, repo));
    }
}