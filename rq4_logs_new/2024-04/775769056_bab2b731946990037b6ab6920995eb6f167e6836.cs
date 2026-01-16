using Microsoft.AspNetCore.Components.Authorization;
using System.Security.Claims;

namespace LagDaemon.YAMUD.WebClient.Services;

public class CustomAuthenticationStateProvider : AuthenticationStateProvider
{
    private readonly IAuthenticationService _authenticationService;

    public CustomAuthenticationStateProvider(IAuthenticationService authenticationService)
    {
        _authenticationService = authenticationService;
        _authenticationService.AuthenticationStateChanged += (sender, args) => NotifyAuthenticationStateChanged();
    }

    public override async Task<AuthenticationState> GetAuthenticationStateAsync()
    {
        var isAuthenticated = await _authenticationService.IsUserAuthenticatedAsync();

        ClaimsIdentity identity;

        if (isAuthenticated)
        {
            var roles = (await _authenticationService.GetUserAsync()).UserRoles;
            var roleClaims = roles.Select(role => new Claim(ClaimTypes.Role, role.ToString()));
            identity = new ClaimsIdentity(new[] 
            { 
                new Claim(ClaimTypes.Name, _authenticationService.Authority.DisplayName),
            }.Concat(roleClaims), "authenticated");
        } else
        {
            identity = new ClaimsIdentity();
        }

        var user = new ClaimsPrincipal(identity);
        return new AuthenticationState(user);
    }

    public void NotifyAuthenticationStateChanged()
    {
        NotifyAuthenticationStateChanged(GetAuthenticationStateAsync());
    }
}