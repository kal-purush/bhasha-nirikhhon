using System.Security.Claims;
using System.Text.Encodings.Web;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

using Zastai.NuGet.Server.Services;

namespace Zastai.NuGet.Server.Auth;

/// <summary>An authentication handler for NuGet API keys.</summary>
public class ApiKeyAuthenticationHandler : AuthenticationHandler<ApiKeyAuthenticationOptions> {

  /// <summary>Creates a new authentication handler for NuGet API keys.</summary>
  public ApiKeyAuthenticationHandler(IOptionsMonitor<ApiKeyAuthenticationOptions> options, ILoggerFactory logger,
                                     UrlEncoder encoder, ISystemClock clock, IApiKeyStore keys, IUserStore users)
    : base(options, logger, encoder, clock) {
    this._keys = keys;
    this._users = users;
  }

  private readonly IApiKeyStore _keys;

  private readonly IUserStore _users;

  /// <inheritdoc />
  protected override async Task<AuthenticateResult> HandleAuthenticateAsync() {
    var req = this.Request;
    if (!req.Headers.TryGetValue(ApiKeySupport.Header, out var apiKeys) || apiKeys.Count <= 0) {
      this.Logger.LogWarning("No API key specified for '{m} {p}'.", req.Method, req.Path);
      return AuthenticateResult.NoResult();
    }
    if (apiKeys.Count != 1) {
      this.Logger.LogWarning("Too many API keys ({c}) specified for '{m} {p}'.", apiKeys.Count, req.Method, req.Path);
      return AuthenticateResult.NoResult();
    }
    var apiKeyString = apiKeys[0];
    if (string.IsNullOrWhiteSpace(apiKeyString)) {
      this.Logger.LogWarning("Null/blank API key specified for '{m} {p}'.", req.Method, req.Path);
      return AuthenticateResult.NoResult();
    }
    var apiKey = await this._keys.FindKeyAsync(apiKeyString);
    if (apiKey is null) {
      this.Logger.LogError("Invalid API key ({k}) specified for '{m} {p}'.", apiKeyString, req.Method, req.Path);
      return AuthenticateResult.Fail("Invalid API key.");
    }
    if (this.Clock.UtcNow > apiKey.Expiry) {
      this.Logger.LogError("Expired API key ({k}) specified for '{m} {p}'.", apiKeyString, req.Method, req.Path);
      return AuthenticateResult.Fail("Expired API key.");
    }
    this.Logger.LogInformation("API key '{n}' ({k}) specified for '{m} {p}'.", apiKey.Name, apiKey.Id, req.Method, req.Path);
    var user = await this._users.FindUserAsync(apiKey.Owner);
    if (user is null) {
      this.Logger.LogError("API key '{n}' ({k}) specifies a nonexistent owner ({o}).", apiKey.Name, apiKey.Id, apiKey.Owner);
      return AuthenticateResult.Fail("Invalid user.");
    }
    this.Logger.LogInformation("API key '{n}' ({k}) belongs to user '{u}' ({o}).", apiKey.Name, apiKey.Id, user.Id, user.Name);
    var claims = new List<Claim> {
      new(ClaimTypes.Actor, user.Id),
      new(ClaimTypes.Name, user.Name),
      // User could be extended to include some other claims, like e-mail, country, ...
    };
    if (apiKey.CanPublish && user.CanPublish) {
      claims.Add(new Claim(ClaimTypes.Role, NuGetRoles.CanPublishPackages));
      claims.Add(new Claim(ClaimTypes.Role, NuGetRoles.CanPublishSymbols));
    }
    if (apiKey.CanDelete && user.CanDelete) {
      claims.Add(new Claim(ClaimTypes.Role, NuGetRoles.CanDeletePackages));
    }
    var principal = new ClaimsPrincipal(new List<ClaimsIdentity> { new(claims, ApiKeySupport.Scheme) });
    return AuthenticateResult.Success(new AuthenticationTicket(principal, ApiKeySupport.Scheme));
  }

}