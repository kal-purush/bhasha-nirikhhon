package cn.xfakir.xmall.common.uitl;

import cn.hutool.core.util.StrUtil;

import javax.crypto.spec.SecretKeySpec;

public class Auth {
    public final String accessKey;
    private final String secretKey;

    private Auth(String accessKey,String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public static Auth create(String accessKey, String secretKey) {
        if (StrUtil.isEmptyOrUndefined(accessKey) || StrUtil.isEmptyOrUndefined(secretKey)) {
            throw new IllegalArgumentException("empty key");
        }
        byte[] sk = StrUtil.utf8Bytes(secretKey);
        SecretKeySpec secretKeySpec = new SecretKeySpec(sk,"HmacSHA1");
        return new Auth(accessKey,secretKey);
    }
}