package rage.ts.callback;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;

@Component
public class CallbackHandler {
    private static final String CALLBACK_TOKEN_PLACEHOLDER = "{{callbackToken}}";
    private static final MediaType JSON_PAYLOAD = MediaType.get("application/json; charset=utf-8");

    private static final Logger log = LoggerFactory.getLogger(CallbackHandler.class);

    private OkHttpClient client;
    private String callbackUrl;
    private String payloadTemplate;

    @Autowired
    public CallbackHandler(
            @Value("${callback.url:}") String callbackUrl,
            @Value("${callback.payloadTemplate:}") String payloadTemplate) {
        client = null;

        if (!StringUtils.hasText(callbackUrl)) {
            log.warn("Callback URL is not specified, callback-handling will be disabled.");
        }
        else if (!StringUtils.hasText(payloadTemplate)) {
            log.warn("Payload template is not specified, callback-handling will be disabled.");
        }
        else if (!payloadTemplate.contains(CALLBACK_TOKEN_PLACEHOLDER)){
            log.warn("Payload template doesn't contain placeholder for callback-token: " + CALLBACK_TOKEN_PLACEHOLDER);
            log.warn("Callback-handling will be disabled.");
        }
        else {
            this.client = new OkHttpClient();
            this.callbackUrl = callbackUrl;
            this.payloadTemplate = payloadTemplate;
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public boolean notifyCallbackForSessionToken(String callbackToken) {
        if (client != null) {
            String payloadWithToken = payloadTemplate.replace(CALLBACK_TOKEN_PLACEHOLDER, callbackToken);

            RequestBody body = RequestBody.create(payloadWithToken, JSON_PAYLOAD);
            Request request = new Request.Builder()
                    .url(callbackUrl)
                    .post(body)
                    .build();
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    return true;
                }
                else {
                    log.warn("Callback executed, but server returned error.");
                    return false;
                }
            }
            catch (IOException e) {
                log.debug("Error executing callback.", e);
            }
        }

        return false;
    }

}