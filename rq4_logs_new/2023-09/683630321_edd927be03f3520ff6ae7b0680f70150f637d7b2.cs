using Common.Dtos;
using Common.Settings;
using Web.User.Helpers;
using Web.User.Services;

namespace Web.User.Middlewares
{
    public class AuthRefreshMiddleware
    {
        private readonly RequestDelegate next;
        private readonly AuthService _authService;
        private readonly AuthHelper _authHelper;
        private readonly CacheHelper _cacheHelper;

        public AuthRefreshMiddleware(RequestDelegate next, AuthService authService, AuthHelper authHelper, CacheHelper cacheHelper)
        {
            this.next = next;
            _authService = authService;
            _authHelper = authHelper;
            _cacheHelper = cacheHelper;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            var principal = context.User;
            if (!(principal.Identity?.IsAuthenticated ?? false))
            {
                var key = context.Session.GetString(Constants.AuthenticationCacheKey);
                if (!string.IsNullOrEmpty(key))
                {
                    var loginData = _cacheHelper.Get<LoginResponseDto>(key);
                    if (loginData != null)
                    {
                        var loginResponse = await _authService.RefreshAsync(loginData.RefreshToken, CancellationToken.None);
                        if (loginResponse.Success)
                        {
                            var authenticatedPrincipal = await _authHelper.SignInAsync(loginResponse.AccessToken, loginResponse.ExpiresAt, context);
                            _cacheHelper.Set<LoginResponseDto>(key, loginResponse);
                            context.User = authenticatedPrincipal;
                        }
                    }
                }
            }
            await next(context);
        }
    }
}