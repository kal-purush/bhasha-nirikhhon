package com.kapeta.spring.rest;

import com.kapeta.spring.config.JWKPublicKeySetProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Slf4j
public class KapetaJwksRestController {
    public static final String PATH_WELL_KNOWN_JWKS = "/.well-known/jwks.json";
    private final JWKPublicKeySetProvider publicKeyProvider;

    public KapetaJwksRestController(JWKPublicKeySetProvider publicKeyProvider) {
        this.publicKeyProvider = publicKeyProvider;
        log.debug("Initializing Kapeta JWKS Rest Controller");
    }

    @GetMapping(PATH_WELL_KNOWN_JWKS)
    public Map<String, Object> kapetaAuthenticationMetadata() {
        return publicKeyProvider.get().toJSONObject();
    }
}