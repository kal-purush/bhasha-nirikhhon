using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace TrekkingForCharity.Web.Features.Account
{
    public class AuthenticationController : Controller
    {
        public async Task Login(string returnUrl = "/")
        {
            await this.HttpContext.ChallengeAsync(
                "Auth0",
                new AuthenticationProperties { RedirectUri = returnUrl });
        }

        [Authorize]
        public async Task Logout()
        {
            await this.HttpContext.SignOutAsync("Auth0", new AuthenticationProperties
            {
                RedirectUri = this.Url.Action("Index", "Home"),
            });

            await this.HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        }
    }
}