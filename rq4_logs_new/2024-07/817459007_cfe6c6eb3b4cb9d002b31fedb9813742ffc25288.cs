using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Components.Authorization;
using System.Security.Claims;

namespace BlazorEcommerce.Client.Authorization
{
    public class EcommerceAuthenticationStateProvider : AuthenticationStateProvider
    {
        public EcommerceAuthenticationStateProvider()
        {
        }

        public override async Task<AuthenticationState> GetAuthenticationStateAsync()
        {
           var  identity = new ClaimsIdentity();
            return new AuthenticationState(new ClaimsPrincipal(identity));
        }
    }
}