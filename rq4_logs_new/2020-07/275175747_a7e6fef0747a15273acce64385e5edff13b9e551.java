package jm.auth;

import com.github.scribejava.apis.VkontakteApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;
import jm.User;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Component
public class VkAuthorisation {

    public static final String PROTECTED_RESOURCE_URL = "https://api.vk.com/method/users.get?v=" + VkontakteApi.VERSION;
    final String clientId = "7499839";
    final String clientSecret = "yTAv4wjNeGubFJExIO72";
    final String customScope = "email";

    @Getter
    final OAuth20Service service = new ServiceBuilder(clientId)

            .apiSecret(clientSecret)
            .defaultScope(customScope)
            .responseType("token")
            .callback("http://localhost:8080/authorization/returnCodeVK")
            .build(VkontakteApi.instance());

    @Getter
    final String authorizationUrl = service.createAuthorizationUrlBuilder()
            .scope(customScope)
            .build();

    public OAuth2AccessToken toGetTokenVK(String code) throws InterruptedException, ExecutionException, IOException {
        return service.getAccessToken(AccessTokenRequestParams.create(code).scope(customScope));
    }

    public User toCreateUser(OAuth2AccessToken token, String email) throws InterruptedException, ExecutionException {
        final OAuthRequest request = new OAuthRequest(Verb.GET, PROTECTED_RESOURCE_URL);
        service.signRequest(token, request);
        User user = null;

        try (Response response = service.execute(request)) {
            JSONObject jsonObj = new JSONObject(response.getBody());
            JSONArray jArray = jsonObj.getJSONArray("response");
            String password = jArray.getJSONObject(0).optString("id");
            String firstName = jArray.getJSONObject(0).optString("first_name");
            String lastName = jArray.getJSONObject(0).optString("last_name");
            String username = firstName + " " + lastName;
            user = new User(firstName, lastName, password);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
    }
}