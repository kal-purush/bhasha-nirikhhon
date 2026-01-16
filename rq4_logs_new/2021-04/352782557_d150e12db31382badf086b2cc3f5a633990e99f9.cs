using AspNet.Security.OpenIdConnect.Primitives;
using AutoMapper;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using OpenIddict.Abstractions;
using OpenIddict.Server.AspNetCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using WayVid.Database.Entity;
using WayVid.Infrastructure.Enum;
using WayVid.Infrastructure.Model;

namespace WayVid.Controllers
{

    [ApiController]
    [Route("[controller]")]
    public class AuthorizationController : ControllerBase
    {
        private readonly SignInManager<User> signInManager;
        private readonly UserManager<User> userManager;
        private readonly IMapper mapper;

        public AuthorizationController(SignInManager<User> signInManager, UserManager<User> userManager, IMapper mapper)
        {
            this.signInManager = signInManager;
            this.userManager = userManager;
            this.mapper = mapper;
        }

        [HttpPost("~/connect/token"), Produces("application/json")]
        public async Task<IActionResult> Exchange()
        {
            OpenIddictRequest request = HttpContext.GetOpenIddictServerRequest();
            if (request.IsPasswordGrantType())
            {
                return await ProceedPasswordGrantTypeAsync(request);
            }

            else if (request.IsRefreshTokenGrantType())
            {
                return await ProceedRefreshTokenGrantTypeAsync(request);
            }
            return BadRequest(new OpenIdConnectResponse
            {
                Error = OpenIdConnectConstants.Errors.InvalidGrant,
                ErrorDescription = "The specified grant type is not supported."
            });
        }

        [HttpGet("~/connect/userinfo"), Produces("application/json")]
        public async Task<IActionResult> GetUserInfo()
        {
            AuthenticateResult authRes = await HttpContext.AuthenticateAsync(OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
            if (authRes.Succeeded)
            {
                return Ok(mapper.Map<UserModel>(await userManager.GetUserAsync(authRes.Principal)));
            }
            return BadRequest(new OpenIdConnectResponse
            {
                Error = OpenIdConnectConstants.Errors.InvalidGrant,
                ErrorDescription = "The specified grant type is not supported."
            });
        }

        private async Task<IActionResult> ProceedPasswordGrantTypeAsync(OpenIddictRequest request)
        {
            User user = await userManager.FindByNameAsync(request.Username);
            if (user == null)
            {
                return BadRequest(new OpenIdConnectResponse
                {
                    Error = OpenIdConnectConstants.Errors.InvalidGrant,
                    ErrorDescription = "Username or password do not match"
                });
            }

            if (!await signInManager.CanSignInAsync(user))
            {
                return BadRequest(new OpenIdConnectResponse
                {
                    Error = OpenIdConnectConstants.Errors.InvalidGrant,
                    ErrorDescription = "The user is no longer allowed to sign in."
                });
            }

            var identity = new ClaimsIdentity(
                OpenIddictServerAspNetCoreDefaults.AuthenticationScheme,
                OpenIdConnectConstants.Claims.Name,
                OpenIdConnectConstants.Claims.Role);

            identity.AddClaim(OpenIdConnectConstants.Claims.Subject,
                user.Id.ToString(),
                OpenIdConnectConstants.Destinations.AccessToken);
            identity.AddClaim(OpenIdConnectConstants.Claims.Name,
                user.UserName,
                OpenIdConnectConstants.Destinations.AccessToken);

            IList<string> userRoles = await userManager.GetRolesAsync(user);
            foreach (string userRole in userRoles)
            {
                identity.AddClaim(OpenIdConnectConstants.Claims.Role, userRole,
                OpenIdConnectConstants.Destinations.AccessToken);
            }

            var principal = new ClaimsPrincipal(identity);
            principal.SetScopes(request.GetScopes());
            return SignIn(principal, new AuthenticationProperties()
            {
                ExpiresUtc = DateTimeOffset.UtcNow.AddHours(1),
                IsPersistent = false,
                IssuedUtc = DateTimeOffset.UtcNow,
                AllowRefresh = true

            }, OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
        }

        private async Task<IActionResult> ProceedRefreshTokenGrantTypeAsync(OpenIddictRequest request)
        {
            AuthenticateResult authRes = await HttpContext.AuthenticateAsync(OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
            return SignIn(authRes.Principal, new AuthenticationProperties()
            {
                ExpiresUtc = DateTimeOffset.UtcNow.AddHours(1),
                IsPersistent = false,
                IssuedUtc = DateTimeOffset.UtcNow,
                AllowRefresh = true

            }, OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
        }
    }
}