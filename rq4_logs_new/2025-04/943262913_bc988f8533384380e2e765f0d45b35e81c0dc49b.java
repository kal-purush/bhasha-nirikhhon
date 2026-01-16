package com.sign.application.usecase;

import com.sign.application.repository.PasskeyRepository;
import com.sign.dto.PasskeyAssertionResult;
import com.yubico.webauthn.AssertionRequest;
import com.yubico.webauthn.AssertionResult;
import com.yubico.webauthn.FinishAssertionOptions;
import com.yubico.webauthn.RelyingParty;
import com.yubico.webauthn.StartAssertionOptions;
import com.yubico.webauthn.data.AuthenticatorAssertionResponse;
import com.yubico.webauthn.data.ByteArray;
import com.yubico.webauthn.data.ClientAssertionExtensionOutputs;
import com.yubico.webauthn.data.PublicKeyCredential;
import com.yubico.webauthn.exception.AssertionFailedException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PasskeyAssertionUseCase {

    private final RelyingParty relyingParty;
    private final PasskeyRepository passkeyRepository;

    public AssertionRequest start() {
        return relyingParty.startAssertion(StartAssertionOptions.builder().build());
    }

    public PasskeyAssertionResult finish(AssertionRequest request,
                                         PublicKeyCredential<AuthenticatorAssertionResponse, ClientAssertionExtensionOutputs> credential) {
        try {
            AssertionResult result = relyingParty.finishAssertion(
                    FinishAssertionOptions.builder()
                            .request(request)
                            .response(credential)
                            .build());
            if (result.isSuccess()) {
                String email = result.getUsername();
                ByteArray credentialId = result.getCredential().getCredentialId();
                long newSignatureCount = result.getSignatureCount();
                passkeyRepository.updateSignatureCount(email, credentialId, newSignatureCount);
                return PasskeyAssertionResult.success(email);
            }
            return PasskeyAssertionResult.failure("패스키 검증에 실패했습니다.");
        } catch (AssertionFailedException e) {
            return PasskeyAssertionResult.failure("패스키 검증에 실패했습니다.");
        }
    }
}