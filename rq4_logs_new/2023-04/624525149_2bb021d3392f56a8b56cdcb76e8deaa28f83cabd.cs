using Microsoft.AspNetCore.Mvc.Filters;

namespace TG.Backend.Filters
{
    /// <summary>
    /// Filtr blokujacy operacje dla zablokowanego uzytkownika (na skutek wprowadzenia hasla w sposob niewlasciwy)
    /// </summary>
    public class ValidateAccountNotLockedFilter : IAsyncActionFilter
    {
        private readonly UserManager<AppUser> _userManager;

        public ValidateAccountNotLockedFilter(UserManager<AppUser> userManager)
        {
            _userManager = userManager;
        }

        public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
        {
            context.ActionArguments.TryGetValue("appUser", out object? appUserObj);

            if (appUserObj is null)
            {
                if (!context.HttpContext.Response.HasStarted)
                {
                    context.HttpContext.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await context.HttpContext.Response.WriteAsync(string.Empty);
                }
            }

            IAppUserDTO appUser = (IAppUserDTO)appUserObj!;

            AppUser user = await _userManager.FindByEmailAsync(appUser.Email);

            if (user is null)
            {
                if (!context.HttpContext.Response.HasStarted)
                {
                    context.HttpContext.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await context.HttpContext.Response.WriteAsync(string.Empty);
                }
            }

            bool isUserLocked = await _userManager.IsLockedOutAsync(user);

            if (isUserLocked)
            {
                context.HttpContext.Response.StatusCode = StatusCodes.Status423Locked;
                await context.HttpContext.Response.WriteAsync(string.Empty);
            }

            await next();
        }
    }
}