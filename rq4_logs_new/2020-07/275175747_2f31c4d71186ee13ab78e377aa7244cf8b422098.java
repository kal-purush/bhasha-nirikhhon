package jm.stockx.apple;

import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import jm.stockx.apple.model.ApplePublicKey;
import jm.stockx.apple.model.ErrorResponse;
import jm.stockx.apple.model.ListApplePublicKey;
import jm.stockx.apple.model.TokenResponse;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Date;
import java.util.Optional;

@Service
public class AppleIdAuthorization {

    //todo настройки при регистации приложения от apple. прописать в application.properties
    @Value("${apple.key-id}")
    private String KEY_ID;

    @Value("${apple.team-id}")
    private String TEAM_ID;

    @Value("${apple.client-id}")
    private String CLIENT_ID;

    private final String APPLE_ID_URL = "https://appleid.apple.com";
    private final String APPLE_TOKEN_URL = "https://appleid.apple.com/auth/token";
    private final String APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys";
    private final String APPLE_AUTH_URL = "https://appleid.apple.com/auth/authorize";
    //todo В документации указано, что параметр redirect_uri:
    // (Required) The URI to which the authorization redirects. It must include a domain name, and can’t be an IP address or localhost.
    private final String REDIRECT_URI = "http://localhost:8080/authorization/appleReturnCode";
    private final String RESPONSE_TYPE = "code,id_token";
    private final String SCOPES = "name,email";
    private PrivateKey pKey;

    private PrivateKey getPrivateKey() throws Exception {
        //todo путь к физическому файлу-ключу, полученному при регистации приложения от apple
        String path = new ClassPathResource("apple/AuthKey.p8").getFile().getAbsolutePath();

        PEMParser pemParser = new PEMParser(new FileReader(path));
        PrivateKeyInfo object = (PrivateKeyInfo) pemParser.readObject();
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        return converter.getPrivateKey(object);
    }

    private String generateJWT() throws Exception {
        pKey = getPrivateKey();
        return Jwts.builder()
                .setHeaderParam(JwsHeader.KEY_ID, KEY_ID)
                .setIssuer(TEAM_ID)
                .setAudience(APPLE_ID_URL)
                .setSubject(CLIENT_ID)
                .setExpiration(new Date(System.currentTimeMillis() + (1000 * 60 * 5)))
                .setIssuedAt(new Date(System.currentTimeMillis()))
                .signWith(SignatureAlgorithm.ES256, pKey)
                .compact();
    }

    private PublicKey createPublicKeyApple(String keyIdentifier) throws Exception {
        HttpResponse<ListApplePublicKey> response = Unirest.get(APPLE_KEYS_URL).asObject(ListApplePublicKey.class);
        if (response.getStatus() != 200) {
            throw new RuntimeException("Error getting public keys from Apple.");
        }
        ListApplePublicKey applePublicKeysList = response.getBody();
        Optional<ApplePublicKey> applePublicKey = applePublicKeysList.getKeys().stream()
                .filter(publicKey -> keyIdentifier.equals(publicKey.getKid()))
                .findFirst();
        if (!applePublicKey.isPresent()) throw new Exception("Not matching key");
        BigInteger modulus = new BigInteger(1, Base64.getUrlDecoder().decode(applePublicKey.get().getN()));
        BigInteger exponent = new BigInteger(1, Base64.getUrlDecoder().decode(applePublicKey.get().getE()));
        return KeyFactory.getInstance(applePublicKey.get().getKty()).generatePublic(new RSAPublicKeySpec(modulus, exponent));
    }

    private Claims getClaims(String keyIdentifier, String idToken) throws Exception {
        PublicKey publicKey = createPublicKeyApple(keyIdentifier);
        try {
            return Jwts.parser().setSigningKey(publicKey).parseClaimsJws(idToken).getBody();
        } catch(Exception e) {
            throw new Exception("Not a right public key");
        }
    }

    public Claims getClimesFromApple(String authorizationCode, String keyIdentifier) throws Exception {
        String token = generateJWT();
        HttpResponse<String> response = Unirest.post(APPLE_TOKEN_URL)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .field("client_id", CLIENT_ID)
                .field("client_secret", token)
                .field("grant_type", "authorization_code")
                .field("code", authorizationCode)
                .asString();
        if (response.getStatus() == 200) {
            TokenResponse tokenResponse = new Gson().fromJson(response.getBody(), TokenResponse.class);
            String idToken = tokenResponse.getIdToken();
            return getClaims(keyIdentifier, idToken);
        } else {
            ErrorResponse errorResponse = new Gson().fromJson(response.getBody(), ErrorResponse.class);
            String error = errorResponse.getError();
            return null;
        }
    }

    public String getAuthorizationUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(APPLE_AUTH_URL);
        sb.append("?client_id=");
        sb.append(CLIENT_ID);
        sb.append("&redirect_uri=");
        sb.append(REDIRECT_URI);
        sb.append("&response_type=");
        sb.append(RESPONSE_TYPE);
        sb.append("&scope=");
        sb.append(SCOPES);
        return sb.toString();
    }
}