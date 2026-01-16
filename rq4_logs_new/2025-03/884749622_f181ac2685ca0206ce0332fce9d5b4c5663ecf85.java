package se.digg.eudiw.util;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.digg.wallet.datatypes.common.TokenSigningAlgorithm;
import se.digg.wallet.datatypes.sdjwt.JSONUtils;
import se.swedenconnect.security.credential.PkiCredential;


public class IssuerStateBuilder {


    private static final Logger logger = LoggerFactory.getLogger(IssuerStateBuilder.class);
    Map<String, Object> payload = new HashMap<String, Object>();
    Calendar issCalendar;
    JWSSigner signer;
    JWK publicJWK;

    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public IssuerStateBuilder(String iss, PkiCredential credential) {
        Date now = new Date();
        issCalendar = Calendar.getInstance();
        issCalendar.setTime(now);
        payload.put("iss", iss);
        payload.put("iat", now);
        payload.put("nbf", now);

        try {
            publicJWK = JSONUtils.getJWKfromPublicKey(credential.getPublicKey());
            signer  = TokenSigningAlgorithm.fromJWSAlgorithm(JWSAlgorithm.ES256).jwsSigner(credential.getPrivateKey());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public IssuerStateBuilder withExp(int hours) {
        Calendar expCalendar = Calendar.getInstance();
        expCalendar.setTime(issCalendar.getTime());
        expCalendar.add(Calendar.HOUR_OF_DAY, hours);
        payload.put("exp", expCalendar.getTime());
        return this;
    }

    public IssuerStateBuilder with(String key, Object value) {
        payload.put(key, value);
        return this;
    }

    public IssuerStateBuilder with(String key, boolean value) {
        payload.put(key, value);
        return this;
    }

    public IssuerStateBuilder withCredentialOfferId(String credentialOfferId) {
        payload.put("credential_offer_id", credentialOfferId);
        return this;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();



        try {
            JWSObject jwsObject = new JWSObject(
                    new JWSHeader.Builder(JWSAlgorithm.ES256)
                            .keyID(publicJWK.computeThumbprint().toString())
                            .jwk(publicJWK)
                            .type(new JOSEObjectType("JWT"))
                            .build(),
                    new Payload(payload)
            );

            jwsObject.sign(signer);
            sb.append(jwsObject.serialize());
            return sb.toString();
        } catch (Exception e) {
            logger.error("IssuerStateBuilder", e);
            throw new RuntimeException(e);
        }

    }


}