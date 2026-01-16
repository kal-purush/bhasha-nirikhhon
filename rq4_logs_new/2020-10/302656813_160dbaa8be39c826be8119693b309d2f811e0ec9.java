package cn.xfakir.xmall.common.sms;

import cn.hutool.core.util.StrUtil;
import com.qiniu.util.UrlSafeBase64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.security.GeneralSecurityException;

public class Auth {
    public final String accessKey;
    private final SecretKeySpec  secretKey;

    private Auth(String accessKey,SecretKeySpec secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public static Auth create(String accessKey, String secretKey) {
        if (StrUtil.isEmptyOrUndefined(accessKey) || StrUtil.isEmptyOrUndefined(secretKey)) {
            throw new IllegalArgumentException("empty key");
        }
        byte[] sk = StrUtil.utf8Bytes(secretKey);
        SecretKeySpec secretKeySpec = new SecretKeySpec(sk,"HmacSHA1");
        return new Auth(accessKey,secretKeySpec);
    }
    public StringMap authorizationV2(String url, String method, byte[] body, String contentType) {
        String authorization = "Qiniu " + signRequestV2(url, method, body, contentType);
        return new StringMap().put("Authorization", authorization);
    }

    public String signRequestV2(String urlString, String method, byte[] body, String contentType) {
        URI uri = URI.create(urlString);

        Mac mac = createMac();
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("%s %s", method, uri.getPath()));
        if (uri.getQuery() != null) {
            sb.append(String.format("?%s", uri.getQuery()));
        }

        sb.append(String.format("\nHost: %s", uri.getHost()));
        if (uri.getPort() > 0) {
            sb.append(String.format(":%d", uri.getPort()));
        }

        if (contentType != null) {
            sb.append(String.format("\nContent-Type: %s", contentType));
        }

        // body
        sb.append("\n\n");
        if (body != null && body.length > 0 && !StrUtil.isEmptyOrUndefined(contentType)) {
            if (contentType.equals("application/x-www-form-urlencoded")
                    || contentType.equals("application/json")) {
                sb.append(new String(body));
            }
        }

        mac.update(StrUtil.utf8Bytes(sb.toString()));

        String digest = UrlSafeBase64.encodeToString(mac.doFinal());
        return this.accessKey + ":" + digest;
    }

    private Mac createMac() {
        Mac mac;
        try {
            mac = javax.crypto.Mac.getInstance("HmacSHA1");
            mac.init(secretKey);
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
        return mac;
    }
}