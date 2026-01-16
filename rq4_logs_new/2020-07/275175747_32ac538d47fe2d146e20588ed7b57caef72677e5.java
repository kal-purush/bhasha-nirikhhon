package jm.stockx.auth;

import com.github.scribejava.apis.GoogleApi20;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;
import jm.stockx.entity.User;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Component
public class GoogleAuthorization {

    public final String protectedResourceUrl = "https://www.googleapis.com/plus/v1/people/me";
    final String clientId = "347711318147-es57tksl762b63c5dhk7oq47kqmfrvph.apps.googleusercontent.com";
    final String clientSecret = "PwPnyCvAefTCs-kA2DwRt1PR";
    final String customScope = "profile";

    @Getter
    final OAuth20Service service = new ServiceBuilder(clientId)
            .apiSecret(clientSecret)
            .defaultScope(customScope)
            .responseType("token")
            .callback("http://localhost:8080/authorization/returnCodeGoogle")
            .build(GoogleApi20.instance());

    @Getter
    final String authorizationUrl = service.createAuthorizationUrlBuilder()
            .scope(customScope)
            .build();

    public OAuth2AccessToken getGoogleOAuth2AccessToken (String code) throws InterruptedException, ExecutionException, IOException {
        return service.getAccessToken(AccessTokenRequestParams.create(code).scope(customScope));
    }

    public User getGoogleUser(OAuth2AccessToken token, String email) throws InterruptedException, ExecutionException {
        final OAuthRequest request = new OAuthRequest(Verb.GET, protectedResourceUrl);
        service.signRequest(token, request);
        User user = null;

        try (Response response = service.execute(request)) {
            JSONObject jsonObj = new JSONObject(response.getBody());
            JSONArray jArray = jsonObj.getJSONArray("response");
            String password = jArray.getJSONObject(0).optString("sub");
            String firstName = jArray.getJSONObject(0).optString("given_name");
            String lastName = jArray.getJSONObject(0).optString("family_name");
            String username = email;
            user = new User(firstName, lastName, password);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return user;
    }
}