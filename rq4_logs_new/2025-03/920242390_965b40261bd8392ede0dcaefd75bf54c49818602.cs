using API.Extensions;
using API.Interfaces;
using Microsoft.AspNetCore.Mvc.Filters;

namespace API.Helpers;

public class LogUserActivity: IAsyncActionFilter
{
    public async Task OnActionExecutionAsync
        (ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var resultContext = await next();

        if (resultContext.HttpContext.User.Identity?.IsAuthenticated is false)
        {
            return;
        }
        
        var username = resultContext.HttpContext.User.GetUsername();
        
        var repo = resultContext.HttpContext.RequestServices.GetService<IUserRepository>();

        if (repo is null)
        {
            return;
        }
        
        var user = await repo.GetUserByUsernameAsync(username);

        if (user is null)
        {
            return;
        }
        
        user.LastActivity = DateTime.UtcNow;

        await repo.SaveAllAsync();
    }
}