namespace IDPLibrary.Entities.ValueObjects;

// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
public class ConfigurationResponse
{
    #region REQUIRED
    [JsonPropertyName("issuer")]
    public string Issuer { get; set; }

    [JsonPropertyName("authorization_endpoint")]
    public string AuthorizationEndpoint { get; set; }

    [JsonPropertyName("token_endpoint")]
    public string TokenEndpoint { get; set; }

    [JsonPropertyName("jwks_uri")]
    public string JwksUri { get; set; }

    [JsonPropertyName("response_types_supported")]
    public string[] ResponseTypesSupported { get; set; }

    [JsonPropertyName("subject_types_supported")]
    public string[] SubjectTypesSupported { get; set; }

    [JsonPropertyName("id_token_signing_alg_values_supported")]
    public string[] IdTokenSigningAlgValuesSupported { get; set; }



    #endregion

    #region RECOMMENDED
    [JsonPropertyName("userinfo_endpoint")]
    public string UserinfoEndpoint { get; set; }

    [JsonPropertyName("registration_endpoint")]
    public string RegistrationEndpoint { get; set; }

    [JsonPropertyName("scopes_supported")]
    public IList<string> ScopesSupported { get; set; }

    [JsonPropertyName("claims_supported")]
    public string[] ClaimsSupported { get; set; }

    #endregion

    #region OPTIONAL

    [JsonPropertyName("response_modes_supported")]
    public string[] ResponseModesSupported { get; set; }

    [JsonPropertyName("grant_types_supported")]
    public string[] GrantTypesSupported { get; set; }

    [JsonPropertyName("acr_values_supported")]
    public string[] AcrValuesSupported { get; set; }

    [JsonPropertyName("id_token_encryption_alg_values_supported")]
    public string[] IdTokenEncryptionAlgValuesSupported { get; set; }

    [JsonPropertyName("id_token_encryption_enc_values_supported")]
    public string[] IdTokenEncryptionEncValuesSupported { get; set; }

    [JsonPropertyName("userinfo_signing_alg_values_supported")]
    public string[] UserinfoSigningAlgValuesSupported { get; set; }

    [JsonPropertyName("userinfo_encryption_alg_values_supported")]
    public string[] UserinfoEncryptionAlgValuesSupported { get; set; }

    [JsonPropertyName("userinfo_encryption_enc_values_supported")]
    public string[] UserinfoEncryptionEncValuesSupported { get; set; }

    [JsonPropertyName("request_object_signing_alg_values_supported")]
    public string[] RequestObjectSigningAlgValuesSupported { get; set; }

    [JsonPropertyName("request_object_encryption_alg_values_supported")]
    public string[] RequestObjectEncryptionAlgValuesSupported { get; set; }

    [JsonPropertyName("request_object_encryption_enc_values_supported")]
    public string[] RequestObjectEncryptionEncValuesSupported { get; set; }

    [JsonPropertyName("token_endpoint_auth_methods_supported")]
    public string[] TokenEndpointAuthMethodsSupported { get; set; }

    [JsonPropertyName("token_endpoint_auth_signing_alg_values_supported")]
    public string[] TokenEndpointAuthSigningAlgValuesSupported { get; set; }

    [JsonPropertyName("display_values_supported")]
    public string[] DisplayValuesSupported { get; set; }

    [JsonPropertyName("claim_types_supported")]
    public string[] ClaimTypesSupported { get; set; }

    [JsonPropertyName("service_documentation")]
    public string ServiceDocumentation { get; set; }

    [JsonPropertyName("claims_locales_supported")]
    public string[] ClaimsLocalesSupported { get; set; }

    [JsonPropertyName("ui_locales_supported")]
    public string[] UILocalesSupported { get; set; }

    [JsonPropertyName("claims_parameter_supported")]
    public bool ClaimsParameterSupported { get; set; }

    [JsonPropertyName("request_parameter_supported")]
    public bool RequestParameterSupported { get; set; }

    [JsonPropertyName("request_uri_parameter_supported")]
    public bool RequestUriParameterSupported { get; set; }

    [JsonPropertyName("require_request_uri_registration")]
    public bool RequireRequestUriRegistration { get; set; }

    [JsonPropertyName("op_policy_uri")]
    public string OpPolicyUri { get; set; }

    [JsonPropertyName("op_tos_uri")]
    public string OpTosUri { get; set; }

    #endregion

    #region OpenID Provider Configuration Response

    [JsonPropertyName("check_session_iframe")]
    public string CheckSessionIframe { get; set; }

    [JsonPropertyName("end_session_endpoint")]
    public string EndSessionEndpoint { get; set; }

    #endregion
}