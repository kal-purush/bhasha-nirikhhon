package se.digg.eudiw.model.credentialissuer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import se.digg.eudiw.authentication.SwedenConnectPrincipal;

import java.util.Objects;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PendingPreAuthorization {
    CredentialOfferParam credentialOfferParam;
    String redirectUri;
    CodeVerifier codeVerifier;
    String clientId;
    String txCode;
    SwedenConnectPrincipal principal;

    public PendingPreAuthorization() {
    }

    public PendingPreAuthorization(CredentialOfferParam credentialOfferParam, String redirectUri, CodeVerifier codeVerifier, String clientId, String txCode, SwedenConnectPrincipal principal) {
        this.credentialOfferParam = credentialOfferParam;
        this.redirectUri = redirectUri;
        this.codeVerifier = codeVerifier;
        this.clientId = clientId;
        this.txCode = txCode;
        this.principal = principal;
    }

    public CredentialOfferParam getCredentialOfferParam() {
        return credentialOfferParam;
    }

    public void setCredentialOfferParam(CredentialOfferParam credentialOfferParam) {
        this.credentialOfferParam = credentialOfferParam;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public void setRedirectUri(String redirectUri) {
        this.redirectUri = redirectUri;
    }

    public CodeVerifier getCodeVerifier() {
        return codeVerifier;
    }

    public void setCodeVerifier(CodeVerifier codeVerifier) {
        this.codeVerifier = codeVerifier;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTxCode() {
        return txCode;
    }

    public void setTxCode(String txCode) {
        this.txCode = txCode;
    }

    public SwedenConnectPrincipal getPrincipal() {
        return principal;
    }

    public void setPrincipal(SwedenConnectPrincipal principal) {
        this.principal = principal;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PendingPreAuthorization that)) return false;
        return Objects.equals(credentialOfferParam, that.credentialOfferParam) && Objects.equals(redirectUri, that.redirectUri) && Objects.equals(codeVerifier, that.codeVerifier) && Objects.equals(clientId, that.clientId) && Objects.equals(txCode, that.txCode) && Objects.equals(principal, that.principal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentialOfferParam, redirectUri, codeVerifier, clientId, txCode, principal);
    }

    @Override
    public String toString() {
        return "PendingPreAuthorization{" +
                "credentialOfferParam=" + credentialOfferParam +
                ", redirectUri='" + redirectUri + '\'' +
                ", codeVerifier=" + codeVerifier +
                ", clientId='" + clientId + '\'' +
                ", txCode='" + txCode + '\'' +
                ", principal=" + principal +
                '}';
    }
}