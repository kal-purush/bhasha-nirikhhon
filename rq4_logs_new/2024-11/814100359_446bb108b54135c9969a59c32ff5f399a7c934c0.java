package soma.edupiuser.oauth.models;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OAuth2ProviderTest {

    @Test
    void isOauthSuccess() {
        // Given
        String successProvider1 = "google";
        String successProvider2 = "GOOGLE";

        String falseProvider1 = "local";
        String falseProvider2 = "";

        assertThat(OAuth2Provider.isOauth(successProvider1)).isEqualTo(true);
        assertThat(OAuth2Provider.isOauth(successProvider2)).isEqualTo(true);

        assertThat(OAuth2Provider.isOauth(falseProvider1)).isEqualTo(false);
        assertThat(OAuth2Provider.isOauth(falseProvider2)).isEqualTo(false);
    }
}