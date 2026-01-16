package uk.gov.ida.integrationTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jwt.SignedJWT;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.util.Duration;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import uk.gov.ida.integrationTest.support.IntegrationTestHelper;
import uk.gov.ida.integrationTest.support.TestRpAppRule;
import uk.gov.ida.jerseyclient.JerseyClientConfigurationBuilder;
import uk.gov.ida.rp.testrp.Urls;
import uk.gov.ida.rp.testrp.tokenservice.GenerateTokenRequestDto;
import uk.gov.ida.rp.testrp.tokenservice.TokenDto;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.ida.saml.core.test.TestEntityIds.STUB_IDP_ONE;

public class TokenResourceAppRuleTests extends IntegrationTestHelper {
    private static Client client;

    private static final int TOKEN_EPOCH = 5;
    @ClassRule
    public static TestRpAppRule testRp = TestRpAppRule.newTestRpAppRule(
            ConfigOverride.config("clientTrustStoreConfiguration.path", ResourceHelpers.resourceFilePath("ida_truststore.ts")),
            ConfigOverride.config("msaMetadataUri", "http://localhost:"+getMsaStubRule().getPort()+"/metadata"),
            ConfigOverride.config("allowInsecureMetadataLocation", "true"),
            ConfigOverride.config("tokenEpoch", String.valueOf(TOKEN_EPOCH)));

    @BeforeClass
    public static void beforeClass() {
        JerseyClientConfiguration jerseyClientConfiguration = JerseyClientConfigurationBuilder.aJerseyClientConfiguration().withTimeout(Duration.seconds(20)).build();
        client = new JerseyClientBuilder(testRp.getEnvironment()).using(jerseyClientConfiguration).build(TokenResourceAppRuleTests.class.getSimpleName());
    }

    @Test
    public void shouldGenerateToken() throws Exception {
        URI uri = testRp.uri(Urls.GENERATE_TOKEN_PATH);
        DateTime validUntil = DateTime.now().plusDays(5);
        Response response = client
                .target(uri)
                .request()
                .post(Entity.json(new GenerateTokenRequestDto(validUntil, STUB_IDP_ONE)));

        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        String token = response.readEntity(String.class);
        String tokenValue = new JSONObject(token).getString("tokenValue");
        TokenDto tokenDto = new ObjectMapper().readValue(SignedJWT.parse(tokenValue).getPayload().toString(), TokenDto.class);
        assertThat(tokenDto.getValidUntil()).isEqualTo(validUntil);
        assertThat(tokenDto.getIssuedTo()).isEqualTo(STUB_IDP_ONE);
        assertThat(tokenDto.getEpoch()).isEqualTo(TOKEN_EPOCH);
    }
}