package com.sign.controller;

import com.sign.application.usecase.PasskeyAssertionUseCase;
import com.sign.application.usecase.PasskeyRegistrationUseCase;
import com.sign.dto.APIResponse;
import com.sign.dto.PasskeyAssertionResult;
import com.sign.dto.PasskeyRegistrationResult;
import com.yubico.webauthn.AssertionRequest;
import com.yubico.webauthn.data.AuthenticatorAssertionResponse;
import com.yubico.webauthn.data.AuthenticatorAttestationResponse;
import com.yubico.webauthn.data.ClientAssertionExtensionOutputs;
import com.yubico.webauthn.data.ClientRegistrationExtensionOutputs;
import com.yubico.webauthn.data.PublicKeyCredential;
import com.yubico.webauthn.data.PublicKeyCredentialCreationOptions;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/passkey")
public class PasskeyController {

    private final PasskeyRegistrationUseCase passkeyRegistrationUseCase;
    private final PasskeyAssertionUseCase passkeyAssertionUseCase;
    private final ResponseProtector responseProtector;
    private final CookieManager cookieManager;

    @GetMapping("/registration")
    public APIResponse<PasskeyRegistrationResult> startRegistration(@ProtectedRequest String email,
                                                                    HttpServletResponse response) {
        PasskeyRegistrationResult result = passkeyRegistrationUseCase.start(email);
        if (result.getStatus().isSuccess()) {
            String encrypt = responseProtector.encrypt(result.getOptions());
            ResponseCookie registrationChallenge = cookieManager.provide("registration_challenge", encrypt);
            response.setHeader(HttpHeaders.SET_COOKIE, registrationChallenge.toString());
        }
        return new APIResponse<>(
                "패스키 등록 challenge 요청 결과",
                result
        );
    }

    @PostMapping("/registration")
    public APIResponse<PasskeyRegistrationResult> finishRegistration(@ProtectedRequest String email,
                                                                     @ProtectedRequest PublicKeyCredentialCreationOptions options,
                                                                     @RequestBody PublicKeyCredential<AuthenticatorAttestationResponse, ClientRegistrationExtensionOutputs> credential) {
        PasskeyRegistrationResult result = passkeyRegistrationUseCase.finish(options, credential, email);
        return new APIResponse<>(
                "패스키 등록 요청 결과",
                result
        );
    }

    @GetMapping("/assertion")
    public APIResponse<AssertionRequest> startAssertion(HttpServletResponse response) {
        AssertionRequest result = passkeyAssertionUseCase.start();
        String encrypt = responseProtector.encrypt(result);
        ResponseCookie registrationChallenge = cookieManager.provide("assertion_challenge", encrypt);
        response.setHeader(HttpHeaders.SET_COOKIE, registrationChallenge.toString());
        return new APIResponse<>(
                "패스키 검증 challenge 요청 결과",
                result
        );
    }

    @PostMapping("/assertion")
    public APIResponse<PasskeyAssertionResult> finishAssertion(@ProtectedRequest AssertionRequest options,
                                                               @RequestBody PublicKeyCredential<AuthenticatorAssertionResponse, ClientAssertionExtensionOutputs> credential) {
        PasskeyAssertionResult result = passkeyAssertionUseCase.finish(options, credential);
        return new APIResponse<>(
                "패스키 검증 결과",
                result
        );
    }
}