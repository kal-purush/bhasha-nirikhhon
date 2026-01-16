using System.Text.Json.Serialization;

namespace IdentityManagementAPI.ModelResources;

public class GetAccessTokenResponseModel
{
    [JsonPropertyName("access_token")]
    public string AccessToken { get; set; } = null!;
    [JsonPropertyName("expires_in")]
    public int ExpiresIn { get; set; }
    [JsonPropertyName("refresh_expires_in")]
    public int RefreshExpiresIn { get; set; }
    [JsonPropertyName("refresh_token")]
    public string RefreshToken { get; set; } = null!;
    [JsonPropertyName("token_type")]
    public string TokenType { get; set; } = null!;
    [JsonPropertyName("not-before-policy")]
    public int NotBeforePolicy { get; set; }
    [JsonPropertyName("session_state")]
    public string SessionState { get; set; } = null!;
    [JsonPropertyName("scope")]
    public string Scope { get; set; } = null!;
}