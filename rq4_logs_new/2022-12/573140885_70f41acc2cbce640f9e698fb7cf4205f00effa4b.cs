namespace Services;

public class OAuth2TokenService
{
    public string? Token { get; set; }
    public string? RefreshToken { get; set; }
    public int? Expiration { get; set; }

    public bool IsNotValidToken()
    {
        return false;
    }
}

public class OAuth2TokenResponse
{
    public string? access_token { get; set; }
    public string? token_type { get; set; }
    public string? refresh_token { get; set; }
    public int? expires_in { get; set; }
    public string? scope { get; set; }
}